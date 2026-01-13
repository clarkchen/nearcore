//! WebSocket handler for real-time block and transaction subscriptions.
//!
//! This module provides WebSocket endpoints for clients to subscribe to
//! real-time block updates and transaction lifecycle events without polling.

use crate::block_subscription::BlockSubscriptionHub;
use crate::transaction_subscription::{
    TransactionLifecycleEvent, TransactionStage, TransactionSubscriptionHub,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use near_primitives::types::AccountId;
use near_primitives::views::BlockView;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

/// WebSocket request message from client
#[derive(Debug, Deserialize)]
#[serde(tag = "method")]
pub enum WsRequest {
    /// Subscribe to new blocks
    #[serde(rename = "subscribe_blocks")]
    SubscribeBlocks,
    /// Unsubscribe from blocks
    #[serde(rename = "unsubscribe_blocks")]
    UnsubscribeBlocks,
    /// Subscribe to transaction lifecycle events for a specific receiver_id (all stages)
    #[serde(rename = "subscribe_transactions")]
    SubscribeTransactions {
        receiver_id: AccountId,
        /// Which stages to subscribe to (defaults to all if empty)
        #[serde(default)]
        stages: Vec<TransactionStage>,
    },
    /// Subscribe to PENDING transactions only (simplified for debugging)
    #[serde(rename = "subscribe_pending_transactions")]
    SubscribePendingTransactions { receiver_id: AccountId },
    /// Subscribe to CONFIRMED transactions only (simplified for debugging)
    #[serde(rename = "subscribe_confirmed_transactions")]
    SubscribeConfirmedTransactions { receiver_id: AccountId },
    /// Subscribe to EXECUTED transactions only (simplified for debugging)
    #[serde(rename = "subscribe_executed_transactions")]
    SubscribeExecutedTransactions { receiver_id: AccountId },
    /// Unsubscribe from transaction events
    #[serde(rename = "unsubscribe_transactions")]
    UnsubscribeTransactions,
    /// Ping to keep connection alive
    #[serde(rename = "ping")]
    Ping,
}

/// WebSocket response message to client
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum WsResponse<'a> {
    /// Subscription confirmed
    #[serde(rename = "subscribed")]
    Subscribed { subscription_id: &'a str },
    /// Transaction subscription confirmed
    #[serde(rename = "tx_subscribed")]
    TxSubscribed {
        subscription_id: &'a str,
        receiver_id: &'a AccountId,
        stages: &'a [TransactionStage],
    },
    /// Pending transaction subscription confirmed (simplified mode)
    #[serde(rename = "pending_tx_subscribed")]
    PendingTxSubscribed {
        subscription_id: &'a str,
        receiver_id: &'a AccountId,
    },
    /// Confirmed transaction subscription confirmed (simplified mode)
    #[serde(rename = "confirmed_tx_subscribed")]
    ConfirmedTxSubscribed {
        subscription_id: &'a str,
        receiver_id: &'a AccountId,
    },
    /// Executed transaction subscription confirmed (simplified mode)
    #[serde(rename = "executed_tx_subscribed")]
    ExecutedTxSubscribed {
        subscription_id: &'a str,
        receiver_id: &'a AccountId,
    },
    /// Unsubscription confirmed
    #[serde(rename = "unsubscribed")]
    Unsubscribed,
    /// New block notification
    #[serde(rename = "block")]
    Block {
        block: &'a BlockView,
        /// Timestamp when the block was received by this node (unix millis)
        received_at: u64,
    },
    /// Transaction lifecycle event
    #[serde(rename = "transaction")]
    Transaction {
        event: &'a TransactionLifecycleEvent,
    },
    /// Pong response
    #[serde(rename = "pong")]
    Pong,
    /// Error message
    #[serde(rename = "error")]
    Error { message: &'a str },
}

/// Filter for transaction subscription
#[derive(Clone)]
pub struct TxSubscriptionFilter {
    pub receiver_id: AccountId,
    pub stages: HashSet<TransactionStage>,
}

impl TxSubscriptionFilter {
    /// Check if an event matches this filter
    pub fn matches(&self, event: &TransactionLifecycleEvent) -> bool {
        event.receiver_id() == &self.receiver_id
            && (self.stages.is_empty() || self.stages.contains(&event.stage()))
    }
}

/// State shared with WebSocket handlers
#[derive(Clone)]
pub struct WsState {
    pub block_hub: Arc<BlockSubscriptionHub>,
    pub tx_hub: Option<Arc<TransactionSubscriptionHub>>,
}

/// Axum handler for WebSocket upgrade
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Handle an individual WebSocket connection
async fn handle_socket(socket: WebSocket, state: WsState) {
    let (mut sender, mut receiver) = socket.split();

    // Generate a unique subscription ID
    let subscription_id = format!("{:016x}", rand::random::<u64>());

    // Subscribe to block updates
    let mut block_subscriber = state.block_hub.subscribe();

    // Optional transaction subscription
    let mut tx_subscriber = state.tx_hub.as_ref().map(|hub| hub.subscribe());
    let mut tx_filter: Option<TxSubscriptionFilter> = None;
    let has_tx_hub = state.tx_hub.is_some();

    // Send subscription confirmation
    let response = WsResponse::Subscribed {
        subscription_id: &subscription_id,
    };
    if let Ok(json) = serde_json::to_string(&response) {
        if sender.send(Message::Text(json.into())).await.is_err() {
            return;
        }
    }

    tracing::info!(
        target: "jsonrpc",
        %subscription_id,
        "WebSocket client subscribed to blocks"
    );

    loop {
        tokio::select! {
            // Receive new block and push to client
            Some(block) = block_subscriber.recv() => {
                let received_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                let response = WsResponse::Block {
                    block: &block,
                    received_at,
                };

                match serde_json::to_string(&response) {
                    Ok(json) => {
                        if sender.send(Message::Text(json.into())).await.is_err() {
                            tracing::debug!(
                                target: "jsonrpc",
                                %subscription_id,
                                "Failed to send block, client disconnected"
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            target: "jsonrpc",
                            ?e,
                            "Failed to serialize block"
                        );
                    }
                }
            }

            // Receive transaction event and push to client (if subscribed)
            Some(event) = async {
                match tx_subscriber.as_mut() {
                    Some(sub) => sub.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                // Check if this event matches our filter
                if let Some(ref filter) = tx_filter {
                    if filter.matches(&event) {
                        let response = WsResponse::Transaction { event: &event };
                        if let Ok(json) = serde_json::to_string(&response) {
                            tracing::debug!(
                                target: "jsonrpc",
                                %subscription_id,
                                stage = ?event.stage(),
                                tx_hash = %event.tx_hash(),
                                "Sending transaction event to client"
                            );
                            if sender.send(Message::Text(json.into())).await.is_err() {
                                tracing::debug!(
                                    target: "jsonrpc",
                                    %subscription_id,
                                    "Failed to send tx event, client disconnected"
                                );
                                break;
                            }
                        }
                    }
                }
            }

            // Handle messages from client
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(request) = serde_json::from_str::<WsRequest>(&text) {
                            match request {
                                WsRequest::UnsubscribeBlocks => {
                                    let response = WsResponse::Unsubscribed;
                                    if let Ok(json) = serde_json::to_string(&response) {
                                        let _ = sender.send(Message::Text(json.into())).await;
                                    }
                                    break;
                                }
                                WsRequest::Ping => {
                                    let response = WsResponse::Pong;
                                    if let Ok(json) = serde_json::to_string(&response) {
                                        let _ = sender.send(Message::Text(json.into())).await;
                                    }
                                }
                                WsRequest::SubscribeBlocks => {
                                    let response = WsResponse::Subscribed {
                                        subscription_id: &subscription_id,
                                    };
                                    if let Ok(json) = serde_json::to_string(&response) {
                                        let _ = sender.send(Message::Text(json.into())).await;
                                    }
                                }
                                WsRequest::SubscribeTransactions { receiver_id, stages } => {
                                    if has_tx_hub {
                                        let stages_set: HashSet<_> = stages.iter().copied().collect();
                                        let stages_vec: Vec<_> = if stages_set.is_empty() {
                                            vec![TransactionStage::Pending, TransactionStage::Confirmed, TransactionStage::Executed]
                                        } else {
                                            stages_set.iter().copied().collect()
                                        };
                                        tx_filter = Some(TxSubscriptionFilter {
                                            receiver_id: receiver_id.clone(),
                                            stages: stages_set,
                                        });
                                        let response = WsResponse::TxSubscribed {
                                            subscription_id: &subscription_id,
                                            receiver_id: &receiver_id,
                                            stages: &stages_vec,
                                        };
                                        if let Ok(json) = serde_json::to_string(&response) {
                                            let _ = sender.send(Message::Text(json.into())).await;
                                        }
                                        tracing::info!(
                                            target: "jsonrpc",
                                            %subscription_id,
                                            %receiver_id,
                                            "WebSocket client subscribed to transactions"
                                        );
                                    } else {
                                        let response = WsResponse::Error {
                                            message: "Transaction subscription not enabled",
                                        };
                                        if let Ok(json) = serde_json::to_string(&response) {
                                            let _ = sender.send(Message::Text(json.into())).await;
                                        }
                                    }
                                }
                                // Simplified subscription: PENDING only
                                WsRequest::SubscribePendingTransactions { receiver_id } => {
                                    if has_tx_hub {
                                        let mut stages_set = HashSet::new();
                                        stages_set.insert(TransactionStage::Pending);
                                        tx_filter = Some(TxSubscriptionFilter {
                                            receiver_id: receiver_id.clone(),
                                            stages: stages_set,
                                        });
                                        let response = WsResponse::PendingTxSubscribed {
                                            subscription_id: &subscription_id,
                                            receiver_id: &receiver_id,
                                        };
                                        if let Ok(json) = serde_json::to_string(&response) {
                                            let _ = sender.send(Message::Text(json.into())).await;
                                        }
                                        tracing::info!(
                                            target: "jsonrpc",
                                            %subscription_id,
                                            %receiver_id,
                                            "WebSocket client subscribed to PENDING transactions only"
                                        );
                                    } else {
                                        let response = WsResponse::Error {
                                            message: "Transaction subscription not enabled",
                                        };
                                        if let Ok(json) = serde_json::to_string(&response) {
                                            let _ = sender.send(Message::Text(json.into())).await;
                                        }
                                    }
                                }
                                // Simplified subscription: CONFIRMED only
                                WsRequest::SubscribeConfirmedTransactions { receiver_id } => {
                                    if has_tx_hub {
                                        let mut stages_set = HashSet::new();
                                        stages_set.insert(TransactionStage::Confirmed);
                                        tx_filter = Some(TxSubscriptionFilter {
                                            receiver_id: receiver_id.clone(),
                                            stages: stages_set,
                                        });
                                        let response = WsResponse::ConfirmedTxSubscribed {
                                            subscription_id: &subscription_id,
                                            receiver_id: &receiver_id,
                                        };
                                        if let Ok(json) = serde_json::to_string(&response) {
                                            let _ = sender.send(Message::Text(json.into())).await;
                                        }
                                        tracing::info!(
                                            target: "jsonrpc",
                                            %subscription_id,
                                            %receiver_id,
                                            "WebSocket client subscribed to CONFIRMED transactions only"
                                        );
                                    } else {
                                        let response = WsResponse::Error {
                                            message: "Transaction subscription not enabled",
                                        };
                                        if let Ok(json) = serde_json::to_string(&response) {
                                            let _ = sender.send(Message::Text(json.into())).await;
                                        }
                                    }
                                }
                                // Simplified subscription: EXECUTED only
                                WsRequest::SubscribeExecutedTransactions { receiver_id } => {
                                    if has_tx_hub {
                                        let mut stages_set = HashSet::new();
                                        stages_set.insert(TransactionStage::Executed);
                                        tx_filter = Some(TxSubscriptionFilter {
                                            receiver_id: receiver_id.clone(),
                                            stages: stages_set,
                                        });
                                        let response = WsResponse::ExecutedTxSubscribed {
                                            subscription_id: &subscription_id,
                                            receiver_id: &receiver_id,
                                        };
                                        if let Ok(json) = serde_json::to_string(&response) {
                                            let _ = sender.send(Message::Text(json.into())).await;
                                        }
                                        tracing::info!(
                                            target: "jsonrpc",
                                            %subscription_id,
                                            %receiver_id,
                                            "WebSocket client subscribed to EXECUTED transactions only"
                                        );
                                    } else {
                                        let response = WsResponse::Error {
                                            message: "Transaction subscription not enabled",
                                        };
                                        if let Ok(json) = serde_json::to_string(&response) {
                                            let _ = sender.send(Message::Text(json.into())).await;
                                        }
                                    }
                                }
                                WsRequest::UnsubscribeTransactions => {
                                    tx_filter = None;
                                    let response = WsResponse::Unsubscribed;
                                    if let Ok(json) = serde_json::to_string(&response) {
                                        let _ = sender.send(Message::Text(json.into())).await;
                                    }
                                }
                            }
                        } else {
                            let response = WsResponse::Error {
                                message: "Invalid request format",
                            };
                            if let Ok(json) = serde_json::to_string(&response) {
                                let _ = sender.send(Message::Text(json.into())).await;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if sender.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::debug!(
                            target: "jsonrpc",
                            %subscription_id,
                            "Client sent close frame"
                        );
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::debug!(
                            target: "jsonrpc",
                            %subscription_id,
                            ?e,
                            "WebSocket error"
                        );
                        break;
                    }
                    None => {
                        // Stream ended
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    tracing::info!(
        target: "jsonrpc",
        %subscription_id,
        "WebSocket connection closed"
    );
}
