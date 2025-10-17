use alloy_primitives::{hex, B256};
// no local sol! here; events live in hopr_events.rs
use alloy_sol_types::SolEvent;
use eyre::WrapErr;
use futures::TryStreamExt;
use reth_chainspec::EthChainSpec;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_node_builder::NodeTypes;
use reth_primitives::{EthPrimitives, Log as RethLog};
use reth_tracing::tracing::info;
use std::fs;

use crate::indexer::hopr_db::HoprEventsDb;
use crate::indexer::hopr_events::{
    // Announcements
    AddressAnnouncement,
    ChannelBalanceDecreased,
    ChannelBalanceIncreased,
    ChannelClosed,
    ChannelOpened,
    Deregistered,
    DeregisteredByManager,
    DergisteredNodeSafe,
    DomainSeparatorUpdated,
    EligibilityUpdated,
    KeyBinding,
    LedgerDomainSeparatorUpdated,
    NetworkRegistryStatusUpdated,
    OutgoingChannelClosureInitiated,
    // Network registry
    Registered,
    RegisteredByManager,
    // Node safe registry
    RegisteredNodeSafe,
    RequirementUpdated,
    RevokeAnnouncement,
    // Oracles
    TicketPriceUpdated,
    TicketRedeemed,
    WinProbUpdated,
    ANNOUNCEMENTS,
    CHANNEL_CONTRACT_ADDR,
    NETWORK_REGISTRY,
    NODE_SAFE_REGISTRY,
    TICKET_PRICE_ORACLE,
    WINNING_PROBABILITY_ORACLE,
};

/// Hooks into the exex pipeline, storing raw HOPR logs and updating the last indexed height.
pub async fn install<Node: FullNodeComponents>(mut ctx: ExExContext<Node>) -> eyre::Result<()>
where
    Node::Types: NodeTypes<Primitives = EthPrimitives>,
{
    let chain_spec = ctx.config.chain.clone();
    let datadir_args = ctx.config.datadir.clone();
    let chain_datadir = datadir_args
        .datadir
        .clone()
        .unwrap_or_chain_default(chain_spec.chain(), datadir_args.clone());
    let hopr_dir = chain_datadir.as_ref().join("hopr_indexer");
    info!(target: "hopr-indexer", "Creating HOPR indexer directory at: {}", hopr_dir.display());
    fs::create_dir_all(&hopr_dir).wrap_err("failed to create hopr indexer directory")?;
    let db_path = hopr_dir.join("hopr_logs.db");
    info!(target: "hopr-indexer", "Opening HOPR events database at: {}", db_path.display());
    let hopr_db = HoprEventsDb::open(&db_path).wrap_err_with(|| {
        format!(
            "failed to open hopr events database at {}",
            db_path.display()
        )
    })?;

    let t_opened: B256 = ChannelOpened::SIGNATURE_HASH;
    let t_closed: B256 = ChannelClosed::SIGNATURE_HASH;
    let t_bal_inc: B256 = ChannelBalanceIncreased::SIGNATURE_HASH;
    let t_bal_dec: B256 = ChannelBalanceDecreased::SIGNATURE_HASH;
    let t_close_init: B256 = OutgoingChannelClosureInitiated::SIGNATURE_HASH;
    let t_dom: B256 = DomainSeparatorUpdated::SIGNATURE_HASH;
    let t_ledger_dom: B256 = LedgerDomainSeparatorUpdated::SIGNATURE_HASH;
    let t_ticket: B256 = TicketRedeemed::SIGNATURE_HASH;
    // Announcements
    let t_addr_announce: B256 = AddressAnnouncement::SIGNATURE_HASH;
    let t_key_binding: B256 = KeyBinding::SIGNATURE_HASH;
    let t_revoke_announce: B256 = RevokeAnnouncement::SIGNATURE_HASH;
    // Network registry
    let t_registered: B256 = Registered::SIGNATURE_HASH;
    let t_registered_mgr: B256 = RegisteredByManager::SIGNATURE_HASH;
    let t_deregistered: B256 = Deregistered::SIGNATURE_HASH;
    let t_deregistered_mgr: B256 = DeregisteredByManager::SIGNATURE_HASH;
    let t_eligibility_updated: B256 = EligibilityUpdated::SIGNATURE_HASH;
    let t_requirement_updated: B256 = RequirementUpdated::SIGNATURE_HASH;
    let t_netreg_status_updated: B256 = NetworkRegistryStatusUpdated::SIGNATURE_HASH;
    // Node safe registry
    let t_reg_node_safe: B256 = RegisteredNodeSafe::SIGNATURE_HASH;
    let t_derg_node_safe: B256 = DergisteredNodeSafe::SIGNATURE_HASH;
    // Oracles
    let t_ticket_price_updated: B256 = TicketPriceUpdated::SIGNATURE_HASH;
    let t_win_prob_updated: B256 = WinProbUpdated::SIGNATURE_HASH;

    info!(target: "hopr-indexer", "hopr-indexer active");

    while let Some(notification) = ctx.notifications.try_next().await? {
        if let ExExNotification::ChainCommitted { new } = &notification {
            let mut total_in_block = 0usize;
            for (block, receipts) in new.blocks_and_receipts() {
                let n = block.num_hash().number as u64;

                let mut block_matches = 0usize;
                for (tx_index, (_tx, receipt)) in
                    block.body().transactions().zip(receipts.iter()).enumerate()
                {
                    for (log_index, log) in receipt.logs.iter().enumerate() {
                        // Channels contract events
                        if log.address == CHANNEL_CONTRACT_ADDR {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();

                            if topic0 == Some(t_bal_dec) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "ChannelBalanceDecreased",
                                )?;
                                continue;
                            }

                            if topic0 == Some(t_bal_inc) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "ChannelBalanceIncreased",
                                )?;
                                continue;
                            }

                            if topic0 == Some(t_opened) {
                                block_matches += 1;
                                note_event(&hopr_db, n, tx_index, log_index, log, "ChannelOpened")?;
                                continue;
                            }

                            if topic0 == Some(t_closed) {
                                block_matches += 1;
                                note_event(&hopr_db, n, tx_index, log_index, log, "ChannelClosed")?;
                                continue;
                            }

                            if topic0 == Some(t_close_init) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "OutgoingChannelClosureInitiated",
                                )?;
                                continue;
                            }

                            if topic0 == Some(t_dom) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "DomainSeparatorUpdated",
                                )?;
                                continue;
                            }

                            if topic0 == Some(t_ledger_dom) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "LedgerDomainSeparatorUpdated",
                                )?;
                                continue;
                            }

                            if topic0 == Some(t_ticket) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "TicketRedeemed",
                                )?;
                                continue;
                            }

                            // if none matched, ignore
                            continue;
                        }

                        // Announcements
                        if log.address == ANNOUNCEMENTS {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();
                            if topic0 == Some(t_addr_announce) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "AddressAnnouncement",
                                )?;
                                continue;
                            }
                            if topic0 == Some(t_key_binding) {
                                block_matches += 1;
                                note_event(&hopr_db, n, tx_index, log_index, log, "KeyBinding")?;
                                continue;
                            }
                            if topic0 == Some(t_revoke_announce) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "RevokeAnnouncement",
                                )?;
                                continue;
                            }
                            // ignore others on this address
                            continue;
                        }

                        // Network registry
                        if log.address == NETWORK_REGISTRY {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();
                            if topic0 == Some(t_registered) {
                                block_matches += 1;
                                note_event(&hopr_db, n, tx_index, log_index, log, "Registered")?;
                                continue;
                            }
                            if topic0 == Some(t_registered_mgr) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "RegisteredByManager",
                                )?;
                                continue;
                            }
                            if topic0 == Some(t_deregistered) {
                                block_matches += 1;
                                note_event(&hopr_db, n, tx_index, log_index, log, "Deregistered")?;
                                continue;
                            }
                            if topic0 == Some(t_deregistered_mgr) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "DeregisteredByManager",
                                )?;
                                continue;
                            }
                            if topic0 == Some(t_eligibility_updated) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "EligibilityUpdated",
                                )?;
                                continue;
                            }
                            if topic0 == Some(t_requirement_updated) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "RequirementUpdated",
                                )?;
                                continue;
                            }
                            if topic0 == Some(t_netreg_status_updated) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "NetworkRegistryStatusUpdated",
                                )?;
                                continue;
                            }
                            // ignore others on this address
                            continue;
                        }

                        // Node safe registry
                        if log.address == NODE_SAFE_REGISTRY {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();
                            if topic0 == Some(t_reg_node_safe) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "RegisteredNodeSafe",
                                )?;
                                continue;
                            }
                            if topic0 == Some(t_derg_node_safe) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "DergisteredNodeSafe",
                                )?;
                                continue;
                            }

                            // ignore others on this address
                            continue;
                        }

                        // Oracles
                        if log.address == TICKET_PRICE_ORACLE
                            || log.address == WINNING_PROBABILITY_ORACLE
                        {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();
                            if topic0 == Some(t_ticket_price_updated) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "TicketPriceUpdated",
                                )?;
                                continue;
                            }
                            if topic0 == Some(t_win_prob_updated) {
                                block_matches += 1;
                                note_event(
                                    &hopr_db,
                                    n,
                                    tx_index,
                                    log_index,
                                    log,
                                    "WinProbUpdated",
                                )?;
                                continue;
                            }
                            continue;
                        }
                    }
                }
                hopr_db.update_last_indexed_block(n)?;
                if block_matches > 0 {
                    total_in_block += block_matches;
                    info!(target: "hopr-indexer", block = n, matched = block_matches, "Block matched HOPR logs");
                }
            }
            if total_in_block == 0 {
                info!(target: "hopr-indexer", "No matches in committed batch");
            }
            ctx.events
                .send(ExExEvent::FinishedHeight(new.tip().num_hash()))?;
        }
    }
    Ok(())
}

/// Records a matched event in the database while emitting a tracing entry.
fn note_event(
    db: &HoprEventsDb,
    block_number: u64,
    tx_index: usize,
    log_index: usize,
    log: &RethLog,
    event_name: &'static str,
) -> eyre::Result<()> {
    info!(
        target: "hopr-indexer",
        block = block_number,
        data = %hex::encode(&log.data.data),
        "{event}",
        event = event_name
    );
    db.record_raw_log(
        block_number,
        tx_index,
        log_index,
        log.address,
        log.topics(),
        log.data.data.as_ref(),
        event_name,
    )
}
