use alloy_primitives::{hex, B256};
// no local sol! here; events live in hopr_events.rs
use alloy_sol_types::SolEvent;
use futures::TryStreamExt;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_primitives::EthPrimitives;
use reth_node_builder::NodeTypes;
use reth_tracing::tracing::info;


use crate::indexer::hopr_events::{
    ChannelOpened,
    ChannelClosed,
    ChannelBalanceIncreased,
    ChannelBalanceDecreased,
    OutgoingChannelClosureInitiated,
    DomainSeparatorUpdated,
    LedgerDomainSeparatorUpdated,
    TicketRedeemed,
    CHANNEL_CONTRACT_ADDR,
    ANNOUNCEMENTS,
    NETWORK_REGISTRY,
    NODE_SAFE_REGISTRY,
    TICKET_PRICE_ORACLE,
    WINNING_PROBABILITY_ORACLE,
    // Announcements
    AddressAnnouncement,
    KeyBinding,
    RevokeAnnouncement,
    // Network registry
    Registered,
    RegisteredByManager,
    Deregistered,
    DeregisteredByManager,
    EligibilityUpdated,
    RequirementUpdated,
    NetworkRegistryStatusUpdated,
    // Node safe registry
    RegisteredNodeSafe,
    DergisteredNodeSafe,
    NodeSafeDomainSeparatorUpdated,
    // Oracles
    TicketPriceUpdated,
    WinProbUpdated,
};


pub async fn install<Node: FullNodeComponents>(mut ctx: ExExContext<Node>) -> eyre::Result<()>
where
    Node::Types: NodeTypes<Primitives = EthPrimitives>,
{
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
    let t_nodesafe_dom_updated: B256 = NodeSafeDomainSeparatorUpdated::SIGNATURE_HASH;
    // Oracles
    let t_ticket_price_updated: B256 = TicketPriceUpdated::SIGNATURE_HASH;
    let t_win_prob_updated: B256 = WinProbUpdated::SIGNATURE_HASH;

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
                        // Channels contract events
                        if log.address == CHANNEL_CONTRACT_ADDR {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();

                            if address_only {
                                block_matches += 1;
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "HOPR log (address-only)");
                                continue;
                            }

                            if topic0 == Some(t_bal_dec) {
                                block_matches += 1;
                                if let Ok(evt) = ChannelBalanceDecreased::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, amount = %evt.amount, "ChannelBalanceDecreased");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "ChannelBalanceDecreased");
                                }
                                continue;
                            }

                            if topic0 == Some(t_bal_inc) {
                                block_matches += 1;
                                if let Ok(evt) = ChannelBalanceIncreased::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, amount = %evt.amount, "ChannelBalanceIncreased");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "ChannelBalanceIncreased");
                                }
                                continue;
                            }

                            if topic0 == Some(t_opened) {
                                block_matches += 1;
                                if let Ok(evt) = ChannelOpened::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, "ChannelOpened");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "ChannelOpened");
                                }
                                continue;
                            }

                            if topic0 == Some(t_closed) {
                                block_matches += 1;
                                if let Ok(evt) = ChannelClosed::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, "ChannelClosed");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "ChannelClosed");
                                }
                                continue;
                            }

                            if topic0 == Some(t_close_init) {
                                block_matches += 1;
                                if let Ok(evt) = OutgoingChannelClosureInitiated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, closureInitiationTime = %evt.closureInitiationTime, "OutgoingChannelClosureInitiated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "OutgoingChannelClosureInitiated");
                                }
                                continue;
                            }

                            if topic0 == Some(t_dom) {
                                block_matches += 1;
                                if let Ok(evt) = DomainSeparatorUpdated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), newDomainSeparator = %hex::encode(evt.newDomainSeparator.as_slice()), "DomainSeparatorUpdated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "DomainSeparatorUpdated");
                                }
                                continue;
                            }

                            if topic0 == Some(t_ledger_dom) {
                                block_matches += 1;
                                if let Ok(evt) = LedgerDomainSeparatorUpdated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), newLedgerDomainSeparator = %hex::encode(evt.newLedgerDomainSeparator.as_slice()), "LedgerDomainSeparatorUpdated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "LedgerDomainSeparatorUpdated");
                                }
                                continue;
                            }

                            if topic0 == Some(t_ticket) {
                                block_matches += 1;
                                if let Ok(evt) = TicketRedeemed::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, amount = %evt.amount, ticketEpoch = %evt.ticketEpoch, ticketIndex = %evt.ticketIndex, "TicketRedeemed");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "TicketRedeemed");
                                }
                                continue;
                            }

                            continue;
                        }

                        // Announcements
                        if log.address == ANNOUNCEMENTS {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();
                            if address_only {
                                block_matches += 1;
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "Announcements (address-only)");
                                continue;
                            }
                            if topic0 == Some(t_addr_announce) {
                                block_matches += 1;
                                if let Ok(evt) = AddressAnnouncement::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), account = %evt.account, pk_len = evt.publicKey.len(), maddr_len = evt.multiaddr.len(), "AddressAnnouncement");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "AddressAnnouncement");
                                }
                                continue;
                            }
                            if topic0 == Some(t_key_binding) {
                                block_matches += 1;
                                if let Ok(evt) = KeyBinding::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), account = %evt.account, "KeyBinding");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "KeyBinding");
                                }
                                continue;
                            }
                            if topic0 == Some(t_revoke_announce) {
                                block_matches += 1;
                                if let Ok(evt) = RevokeAnnouncement::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), account = %evt.account, "RevokeAnnouncement");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "RevokeAnnouncement");
                                }
                                continue;
                            }
                            // Fallback if unknown announcement event
                            block_matches += 1;
                            info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "Announcements");
                            continue;
                        }

                        // Network registry
                        if log.address == NETWORK_REGISTRY {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();
                            if address_only {
                                block_matches += 1;
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "NetworkRegistry (address-only)");
                                continue;
                            }
                            if topic0 == Some(t_registered) {
                                block_matches += 1;
                                if let Ok(evt) = Registered::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), account = %evt.account, peerId = %evt.peerId, "Registered");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "Registered");
                                }
                                continue;
                            }
                            if topic0 == Some(t_registered_mgr) {
                                block_matches += 1;
                                if let Ok(evt) = RegisteredByManager::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), manager = %evt.manager, account = %evt.account, peerId = %evt.peerId, "RegisteredByManager");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "RegisteredByManager");
                                }
                                continue;
                            }
                            if topic0 == Some(t_deregistered) {
                                block_matches += 1;
                                if let Ok(evt) = Deregistered::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), account = %evt.account, peerId = %evt.peerId, "Deregistered");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "Deregistered");
                                }
                                continue;
                            }
                            if topic0 == Some(t_deregistered_mgr) {
                                block_matches += 1;
                                if let Ok(evt) = DeregisteredByManager::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), manager = %evt.manager, account = %evt.account, peerId = %evt.peerId, "DeregisteredByManager");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "DeregisteredByManager");
                                }
                                continue;
                            }
                            if topic0 == Some(t_eligibility_updated) {
                                block_matches += 1;
                                if let Ok(evt) = EligibilityUpdated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), account = %evt.account, isEligible = evt.isEligible, rank = %evt.rank, "EligibilityUpdated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "EligibilityUpdated");
                                }
                                continue;
                            }
                            if topic0 == Some(t_requirement_updated) {
                                block_matches += 1;
                                if let Ok(evt) = RequirementUpdated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), key = %hex::encode(evt.key.as_slice()), value = %evt.value, "RequirementUpdated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "RequirementUpdated");
                                }
                                continue;
                            }
                            if topic0 == Some(t_netreg_status_updated) {
                                block_matches += 1;
                                if let Ok(evt) = NetworkRegistryStatusUpdated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), isPaused = evt.isPaused, "NetworkRegistryStatusUpdated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "NetworkRegistryStatusUpdated");
                                }
                                continue;
                            }
                            // Fallback
                            block_matches += 1;
                            info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "NetworkRegistry");
                            continue;
                        }

                        // Node safe registry
                        if log.address == NODE_SAFE_REGISTRY {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();
                            if address_only {
                                block_matches += 1;
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "NodeSafeRegistry (address-only)");
                                continue;
                            }
                            if topic0 == Some(t_reg_node_safe) {
                                block_matches += 1;
                                if let Ok(evt) = RegisteredNodeSafe::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), owner = %evt.owner, safe = %evt.safe, "RegisteredNodeSafe");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "RegisteredNodeSafe");
                                }
                                continue;
                            }
                            if topic0 == Some(t_derg_node_safe) {
                                block_matches += 1;
                                if let Ok(evt) = DergisteredNodeSafe::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), owner = %evt.owner, safe = %evt.safe, "DergisteredNodeSafe");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "DergisteredNodeSafe");
                                }
                                continue;
                            }
                            if topic0 == Some(t_nodesafe_dom_updated) {
                                block_matches += 1;
                                if let Ok(evt) = NodeSafeDomainSeparatorUpdated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), newDomainSeparator = %hex::encode(evt.newDomainSeparator.as_slice()), "NodeSafeDomainSeparatorUpdated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "NodeSafeDomainSeparatorUpdated");
                                }
                                continue;
                            }
                            // Fallback
                            block_matches += 1;
                            info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "NodeSafeRegistry");
                            continue;
                        }

                        // Oracles
                        if log.address == TICKET_PRICE_ORACLE || log.address == WINNING_PROBABILITY_ORACLE {
                            let topics = log.topics();
                            let topic0 = topics.first().copied();
                            if address_only {
                                block_matches += 1;
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "Oracle (address-only)");
                                continue;
                            }
                            if topic0 == Some(t_ticket_price_updated) {
                                block_matches += 1;
                                if let Ok(evt) = TicketPriceUpdated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), price = %evt.price, "TicketPriceUpdated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "TicketPriceUpdated");
                                }
                                continue;
                            }
                            if topic0 == Some(t_win_prob_updated) {
                                block_matches += 1;
                                if let Ok(evt) = WinProbUpdated::decode_raw_log(topics, &log.data.data) {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), winProb = %evt.winProb, "WinProbUpdated");
                                } else {
                                    info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "WinProbUpdated");
                                }
                                continue;
                            }
                            // Fallback
                            block_matches += 1;
                            info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "Oracle");
                            continue;
                        }
                        let topics = log.topics();
                        let topic0 = topics.first().copied();

                        if address_only {
                            block_matches += 1;
                            info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "HOPR log (address-only)");
                            continue;
                        }

                        if topic0 == Some(t_bal_dec) {
                            block_matches += 1;
                            if let Ok(evt) = ChannelBalanceDecreased::decode_raw_log(topics, &log.data.data) {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, amount = %evt.amount, "ChannelBalanceDecreased");
                            } else {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "ChannelBalanceDecreased");
                            }
                            continue;
                        }

                        if topic0 == Some(t_bal_inc) {
                            block_matches += 1;
                            if let Ok(evt) = ChannelBalanceIncreased::decode_raw_log(topics, &log.data.data) {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, amount = %evt.amount, "ChannelBalanceIncreased");
                            } else {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "ChannelBalanceIncreased");
                            }
                            continue;
                        }

                        if topic0 == Some(t_opened) {
                            block_matches += 1;
                            if let Ok(evt) = ChannelOpened::decode_raw_log(topics, &log.data.data) {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, "ChannelOpened");
                            } else {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "ChannelOpened");
                            }
                            continue;
                        }

                        if topic0 == Some(t_closed) {
                            block_matches += 1;
                            if let Ok(evt) = ChannelClosed::decode_raw_log(topics, &log.data.data) {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, "ChannelClosed");
                            } else {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "ChannelClosed");
                            }
                            continue;
                        }

                        if topic0 == Some(t_close_init) {
                            block_matches += 1;
                            if let Ok(evt) = OutgoingChannelClosureInitiated::decode_raw_log(topics, &log.data.data) {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, closureInitiationTime = %evt.closureInitiationTime, "OutgoingChannelClosureInitiated");
                            } else {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "OutgoingChannelClosureInitiated");
                            }
                            continue;
                        }

                        if topic0 == Some(t_dom) {
                            block_matches += 1;
                            if let Ok(evt) = DomainSeparatorUpdated::decode_raw_log(topics, &log.data.data) {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), newDomainSeparator = %hex::encode(evt.newDomainSeparator.as_slice()), "DomainSeparatorUpdated");
                            } else {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "DomainSeparatorUpdated");
                            }
                            continue;
                        }

                        if topic0 == Some(t_ledger_dom) {
                            block_matches += 1;
                            if let Ok(evt) = LedgerDomainSeparatorUpdated::decode_raw_log(topics, &log.data.data) {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), newLedgerDomainSeparator = %hex::encode(evt.newLedgerDomainSeparator.as_slice()), "LedgerDomainSeparatorUpdated");
                            } else {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "LedgerDomainSeparatorUpdated");
                            }
                            continue;
                        }

                        if topic0 == Some(t_ticket) {
                            block_matches += 1;
                            if let Ok(evt) = TicketRedeemed::decode_raw_log(topics, &log.data.data) {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), src = %evt.src, dest = %evt.dest, amount = %evt.amount, ticketEpoch = %evt.ticketEpoch, ticketIndex = %evt.ticketIndex, "TicketRedeemed");
                            } else {
                                info!(target: "hopr-indexer", block = n, tx = %tx.hash(), data = %hex::encode(&log.data.data), "TicketRedeemed");
                            }
                            continue;
                        }
                    }
                }
                if block_matches > 0 {
                    total_in_block += block_matches;
                    info!(target: "hopr-indexer", block = n, matched = block_matches, "Block matched HOPR logs");
                }
            }
            if total_in_block == 0 { info!(target: "hopr-indexer", "No matches in committed batch"); }
            ctx.events.send(ExExEvent::FinishedHeight(new.tip().num_hash()))?;
        }
    }
    Ok(())
}
