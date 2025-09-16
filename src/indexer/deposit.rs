use alloy_primitives::{hex, Address, address,B256};
use alloy_sol_macro::sol;
use alloy_sol_types::SolEvent;
use futures::TryStreamExt;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_primitives::EthPrimitives;
use reth_node_builder::NodeTypes;
use reth_tracing::tracing::info;

const DEPOSIT_CONTRACT_ADDR: Address = address!("0xb97036A26259B7147018913bD58a774cf91acf25");

sol! { event DepositEvent(bytes pubkey, bytes withdrawal_credentials, bytes amount, bytes signature, bytes index); }

pub async fn install<Node: FullNodeComponents>(mut ctx: ExExContext<Node>) -> eyre::Result<()>
where
    Node::Types: NodeTypes<Primitives = EthPrimitives>,
{
    let deposit_topic: B256 = DepositEvent::SIGNATURE_HASH;

    // Validation toggles
    let address_only = std::env::var("DEPOSIT_ADDRESS_ONLY").ok().as_deref() == Some("1");
    info!(target: "deposit-indexer", address_only, "deposit-indexer active");

    while let Some(notification) = ctx.notifications.try_next().await? {
        if let ExExNotification::ChainCommitted { new } = &notification {
            let mut total_in_block = 0usize;
            for (block, receipts) in new.blocks_and_receipts() {
                let n = block.num_hash().number as u64;

                let mut block_matches = 0usize;
                for (tx, receipt) in block.body().transactions().zip(receipts.iter()) {
                    for log in &receipt.logs {
                        if log.address != DEPOSIT_CONTRACT_ADDR { continue; }
                        if address_only || log.topics().first().copied() == Some(deposit_topic) {
                            block_matches += 1;
                            info!(
                                target: "deposit-indexer",
                                block = n,
                                tx = %tx.hash(),
                                data = %hex::encode(&log.data.data),
                                "DepositEvent"
                            );
                        }
                    }
                }
                if block_matches > 0 {
                    total_in_block += block_matches;
                    info!(target: "deposit-indexer", block = n, matched = block_matches, "Block matched DepositEvent logs");
                }
            }
            if total_in_block == 0 { info!(target: "deposit-indexer", "No matches in committed batch"); }
            ctx.events.send(ExExEvent::FinishedHeight(new.tip().num_hash()))?;
        }
    }
    Ok(())
}
