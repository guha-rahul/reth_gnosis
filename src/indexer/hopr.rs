use alloy_primitives::{hex, Address, address,B256};
use alloy_sol_macro::sol;
use alloy_sol_types::SolEvent;
use futures::TryStreamExt;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_primitives::EthPrimitives;
use reth_node_builder::NodeTypes;
use reth_tracing::tracing::info;

const CHANNEL_CONTRACT_ADDR: Address = address!("0x693Bac5ce61c720dDC68533991Ceb41199D8F8ae");

sol! { event ChannelBalanceDecreased(bytes32 indexed channelId, uint96 newBalance); }

pub async fn install<Node: FullNodeComponents>(mut ctx: ExExContext<Node>) -> eyre::Result<()>
where
    Node::Types: NodeTypes<Primitives = EthPrimitives>,
{
    let channel_topic: B256 = ChannelBalanceDecreased::SIGNATURE_HASH;

    // Validation toggles
    let address_only = std::env::var("HOPR_ONLY").ok().as_deref() == Some("1");
    info!(target: "hopr-indexer", address_only, "hopr-indexer active");

    while let Some(notification) = ctx.notifications.try_next().await? {
        if let ExExNotification::ChainCommitted { new } = &notification {
            let mut total_in_block = 0usize;
            for (block, receipts) in new.blocks_and_receipts() {
                let n = block.num_hash().number as u64;

                let mut block_matches = 0usize;
                for (tx, receipt) in block.body().transactions().zip(receipts.iter()) {
                    for log in &receipt.logs {
                        if log.address != CHANNEL_CONTRACT_ADDR { continue; }
                        if address_only || log.topics().first().copied() == Some(channel_topic) {
                            block_matches += 1;
                            if let Ok(evt) = ChannelBalanceDecreased::decode_raw_log(log.topics(), &log.data.data) {
                                info!(
                                    target: "hopr-indexer",
                                    block = n,
                                    tx = %tx.hash(),
                                    channel_id = %hex::encode(evt.channelId.as_slice()),
                                    new_balance = %evt.newBalance,
                                    "ChannelBalanceDecreased"
                                );
                            } else {
                                info!(
                                    target: "hopr-indexer",
                                    block = n,
                                    tx = %tx.hash(),
                                    data = %hex::encode(&log.data.data),
                                    "ChannelBalanceDecreased"
                                );
                            }
                        }
                    }
                }
                if block_matches > 0 {
                    total_in_block += block_matches;
                    info!(target: "hopr-indexer", block = n, matched = block_matches, "Block matched ChannelBalanceDecreased logs");
                }
            }
            if total_in_block == 0 { info!(target: "hopr-indexer", "No matches in committed batch"); }
            ctx.events.send(ExExEvent::FinishedHeight(new.tip().num_hash()))?;
        }
    }
    Ok(())
}
