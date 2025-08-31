use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use futures::future::join_all;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Parser, Debug, Clone)]
struct Args {
    /// HTTP endpoint, e.g. http://127.0.0.1:8545
    #[arg(long, default_value = "http://127.0.0.1:8545")]
    endpoint: String,

    /// Hex block number or tag (e.g. latest). Example: 0xC3500
    #[arg(long, default_value = "latest")]
    block: String,

    /// Whether to request full tx objects
    #[arg(long, default_value_t = false)]
    full_txs: bool,

    /// Target requests per second
    #[arg(long, default_value_t = 1000u64)]
    qps: u64,

    /// Duration to run in seconds
    #[arg(long, default_value_t = 10u64)]
    duration_secs: u64,

    /// Concurrency (max in-flight requests)
    #[arg(long, default_value_t = 200usize)]
    concurrency: usize,
}

#[derive(Serialize)]
struct JsonRpcReq<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: (&'a str, bool),
}

#[derive(Deserialize, Debug)]
struct JsonRpcResp<T> {
    #[allow(dead_code)]
    jsonrpc: String,
    #[allow(dead_code)]
    id: u64,
    #[allow(dead_code)]
    result: Option<T>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let client = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(args.concurrency)
        .build()?;

    let target_interval = Duration::from_nanos(1_000_000_000 / args.qps);
    let end_at = Instant::now() + Duration::from_secs(args.duration_secs);
    let semaphore = Arc::new(Semaphore::new(args.concurrency));

    let mut latencies: Vec<u128> = Vec::with_capacity((args.qps * args.duration_secs) as usize);
    let mut sent: u64 = 0;
    let mut done: u64 = 0;

    let mut in_flight = vec![];

    while Instant::now() < end_at {
        let start_tick = Instant::now();

        // rate-limit by QPS and concurrency
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client_ref = client.clone();
        let endpoint = args.endpoint.clone();
        let block = args.block.clone();
        let full = args.full_txs;

        let fut = tokio::spawn(async move {
            let t0 = Instant::now();
            let req = JsonRpcReq { jsonrpc: "2.0", id: 1, method: "eth_getBlockByNumber", params: (&block, full) };
            let res = client_ref
                .post(&endpoint)
                .json(&req)
                .send()
                .await;

            let latency_ms = t0.elapsed().as_micros();
            drop(permit);
            (res, latency_ms)
        });

        in_flight.push(fut);
        sent += 1;

        // try to sleep the remainder to pace at QPS
        let elapsed = start_tick.elapsed();
        if elapsed < target_interval {
            tokio::time::sleep(target_interval - elapsed).await;
        }

        // opportunistically collect finished tasks to keep memory bounded
        if in_flight.len() >= args.concurrency {
            let finished = join_all(in_flight).await;
            in_flight = Vec::new();
            for item in finished {
                if let Ok((Ok(resp), lat)) = item {
                    // Consume body to avoid connection reuse issues
                    let _ = resp.bytes().await;
                    latencies.push(lat);
                    done += 1;
                } else if let Ok((Err(_), lat)) = item {
                    latencies.push(lat);
                    done += 1;
                }
            }
        }
    }

    // drain remaining
    let finished = join_all(in_flight).await;
    for item in finished {
        if let Ok((Ok(resp), lat)) = item {
            let _ = resp.bytes().await;
            latencies.push(lat);
            done += 1;
        } else if let Ok((Err(_), lat)) = item {
            latencies.push(lat);
            done += 1;
        }
    }

    if latencies.is_empty() {
        println!("No results collected.");
        return Ok(());
    }

    latencies.sort_unstable();
    let p = |q: f64| -> f64 {
        let idx = ((latencies.len() as f64 - 1.0) * q).round() as usize;
        latencies[idx] as f64 / 1000.0
    };
    let avg_ms = (latencies.iter().sum::<u128>() as f64 / latencies.len() as f64) / 1000.0;

    println!(
        "sent={} done={} avg_ms={:.3} p50={:.3} p90={:.3} p99={:.3} p99.9={:.3}",
        sent,
        done,
        avg_ms,
        p(0.50),
        p(0.90),
        p(0.99),
        p(0.999),
    );

    Ok(())
}


