use clap::Parser;
use reth_gnosis::indexer::snapshot::SnapshotCreator;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "create_snapshot")]
#[command(about = "Create a tar.xz snapshot from a HOPR indexer database")]
struct Args {
    /// Path to the SQLite database file
    #[arg(short, long)]
    db: PathBuf,

    /// Output path for the tar.xz archive
    #[arg(short, long)]
    output: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let creator = SnapshotCreator::new();
    let size = creator.create_snapshot(&args.db, &args.output)?;

    println!(" Snapshot created: {}", args.output.display());
    println!("  Size: {} bytes ({:.2} MB)", size, size as f64 / 1_024_000.0);

    Ok(())
}
