//! WebSocket handler for real-time block subscriptions.
//!
//! This module provides WebSocket endpoints for clients to subscribe to
//! real-time block updates without polling.

use crate::block_subscription::{BlockPushView, BlockSubscriptionHub};
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures::{Sink, SinkExt, StreamExt};
use near_primitives::types::{AccountId, BlockHeight};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct BlockOut {
    height: BlockHeight,
    transactions: Vec<TxOut>,
}

#[derive(Debug, Serialize)]
pub struct TxOut {
    hash: near_primitives::hash::CryptoHash,
    receiver_id: AccountId,
    signer_id: AccountId,
    actions: Vec<near_primitives::views::ActionView>,
    status: near_primitives::views::ExecutionStatusView,
}

/// WebSocket request message from client
#[derive(Debug, Deserialize)]
#[serde(tag = "method")]
pub enum WsRequest {
    /// Subscribe to new blocks
    #[serde(rename = "subscribe_blocks")]
    SubscribeBlocks { receiver_ids: Vec<AccountId> },
    /// Unsubscribe from blocks
    #[serde(rename = "unsubscribe_blocks")]
    UnsubscribeBlocks,
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
    /// Unsubscription confirmed
    #[serde(rename = "unsubscribed")]
    Unsubscribed,
    /// New block notification
    #[serde(rename = "block")]
    Block {
        block: BlockOut,
        /// Timestamp when the block was received by this node (unix millis)
        received_at: u64,
    },
    /// Pong response
    #[serde(rename = "pong")]
    Pong,
    /// Error message
    #[serde(rename = "error")]
    Error { message: &'a str },
}

/// State shared with WebSocket handlers
#[derive(Clone)]
pub struct WsState {
    pub block_hub: Arc<BlockSubscriptionHub>,
}

/// Axum handler for WebSocket upgrade
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<WsState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.block_hub))
}

/// Handle an individual WebSocket connection
async fn handle_socket(socket: WebSocket, block_hub: Arc<BlockSubscriptionHub>) {
    let (mut sender, mut receiver) = socket.split();

    // Generate a unique subscription ID
    let subscription_id = format!("{:016x}", rand::random::<u64>());

    // Subscribe to block updates
    let mut block_subscriber = block_hub.subscribe();
    let mut block_filters: HashSet<AccountId> = HashSet::new();

    // Send subscription confirmation
    let response = WsResponse::Subscribed { subscription_id: &subscription_id };
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
                if let Some(filtered) = build_filtered_block(&block, &block_filters) {
                    let received_at = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    let response = WsResponse::Block { block: filtered, received_at };

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
                                WsRequest::SubscribeBlocks { receiver_ids } => {
                                    block_filters = receiver_ids.into_iter().collect();
                                    let response =
                                        WsResponse::Subscribed { subscription_id: &subscription_id };
                                    if let Ok(json) = serde_json::to_string(&response) {
                                        let _ = sender.send(Message::Text(json.into())).await;
                                    }
                                }
                            }
                        } else {
                            send_error(&mut sender, "Invalid request format").await;
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

async fn send_error(sender: &mut (impl Sink<Message> + Unpin), message: &'static str) {
    if let Ok(json) = serde_json::to_string(&WsResponse::Error { message }) {
        let _ = sender.send(Message::Text(json.into())).await;
    }
}

fn build_filtered_block(block: &BlockPushView, filters: &HashSet<AccountId>) -> Option<BlockOut> {
    // If no filters provided, do not push anything (avoid noisy defaults)
    if filters.is_empty() {
        return None;
    }

    let transactions: Vec<_> = block
        .transactions
        .iter()
        .filter(|tx| filters.contains(&tx.receiver_id))
        .map(|tx| TxOut {
            hash: tx.hash,
            receiver_id: tx.receiver_id.clone(),
            signer_id: tx.signer_id.clone(),
            actions: tx.actions.clone(),
            status: tx.status.clone(),
        })
        .collect();

    if transactions.is_empty() {
        return None;
    }

    Some(BlockOut { height: block.height, transactions })
}
