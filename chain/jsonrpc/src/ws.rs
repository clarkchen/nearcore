//! WebSocket handler for real-time block and chunk subscriptions.
//!
//! This module provides WebSocket endpoints for clients to subscribe to
//! real-time block and chunk updates without polling.

use crate::block_subscription::BlockSubscriptionHub;
use crate::chunk_subscription::{ChunkSubscriptionHub, ChunkSubscriber};
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures::{Sink, SinkExt, StreamExt, future};
use near_primitives::types::AccountId;
use near_primitives::views::{BlockView, ChunkView};
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
    /// Subscribe to chunks filtered by receiver ids
    #[serde(rename = "subscribe_chunks")]
    SubscribeChunks { receiver_ids: Vec<AccountId> },
    /// Unsubscribe from chunk stream
    #[serde(rename = "unsubscribe_chunks")]
    UnsubscribeChunks,
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
    /// Chunk subscription confirmed
    #[serde(rename = "chunk_subscribed")]
    ChunkSubscribed { subscription_id: &'a str, receiver_ids: &'a [AccountId] },
    /// Unsubscription confirmed
    #[serde(rename = "unsubscribed")]
    Unsubscribed,
    /// Chunk unsubscription confirmed
    #[serde(rename = "chunk_unsubscribed")]
    ChunkUnsubscribed,
    /// New block notification
    #[serde(rename = "block")]
    Block {
        block: &'a BlockView,
        /// Timestamp when the block was received by this node (unix millis)
        received_at: u64,
    },
    /// New chunk notification (filtered by receiver ids)
    #[serde(rename = "chunk")]
    Chunk {
        chunk: &'a ChunkView,
        /// Timestamp when the chunk was received by this node (unix millis)
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
    pub chunk_hub: Option<Arc<ChunkSubscriptionHub>>,
}

/// Axum handler for WebSocket upgrade
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<WsState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.block_hub, state.chunk_hub))
}

/// Handle an individual WebSocket connection
async fn handle_socket(
    socket: WebSocket,
    block_hub: Arc<BlockSubscriptionHub>,
    chunk_hub: Option<Arc<ChunkSubscriptionHub>>,
) {
    let (mut sender, mut receiver) = socket.split();

    // Generate a unique subscription ID
    let subscription_id = format!("{:016x}", rand::random::<u64>());

    // Subscribe to block updates
    let mut block_subscriber = block_hub.subscribe();
    // Lazily create chunk subscriber when requested
    let mut chunk_subscriber: Option<ChunkSubscriber> = None;
    let mut chunk_filters: HashSet<AccountId> = HashSet::new();

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
            // Receive new chunk and push to client (when subscribed)
            chunk_msg = async {
                if let Some(sub) = chunk_subscriber.as_mut() {
                    sub.recv().await
                } else {
                    future::pending().await
                }
            } => {
                if let Some(chunk) = chunk_msg {
                    if let Some(filtered_chunk) = filter_chunk(&chunk, &chunk_filters) {
                        let received_at = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;

                        let response = WsResponse::Chunk { chunk: &filtered_chunk, received_at };
                        match serde_json::to_string(&response) {
                            Ok(json) => {
                                if sender.send(Message::Text(json.into())).await.is_err() {
                                    tracing::debug!(
                                        target: "jsonrpc",
                                        %subscription_id,
                                        "Failed to send chunk, client disconnected"
                                    );
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    target: "jsonrpc",
                                    ?e,
                                    "Failed to serialize chunk"
                                );
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
                                    // Already subscribed, just confirm
                                    let response = WsResponse::Subscribed {
                                        subscription_id: &subscription_id,
                                    };
                                    if let Ok(json) = serde_json::to_string(&response) {
                                        let _ = sender.send(Message::Text(json.into())).await;
                                    }
                                }
                                WsRequest::SubscribeChunks { receiver_ids } => {
                                    let Some(chunk_hub) = &chunk_hub else {
                                        send_error(&mut sender, "Chunk subscription not enabled").await;
                                        continue;
                                    };

                                    let filters: HashSet<_> = receiver_ids.into_iter().collect();
                                    if filters.is_empty() {
                                        send_error(&mut sender, "receiver_ids must not be empty").await;
                                        continue;
                                    }

                                    let response_ids: Vec<_> = filters.iter().cloned().collect();
                                    chunk_filters = filters;
                                    if chunk_subscriber.is_none() {
                                        chunk_subscriber = Some(chunk_hub.subscribe());
                                    }
                                    let response = WsResponse::ChunkSubscribed {
                                        subscription_id: &subscription_id,
                                        receiver_ids: &response_ids,
                                    };
                                    if let Ok(json) = serde_json::to_string(&response) {
                                        let _ = sender.send(Message::Text(json.into())).await;
                                    }
                                }
                                WsRequest::UnsubscribeChunks => {
                                    chunk_subscriber = None;
                                    chunk_filters.clear();
                                    let response = WsResponse::ChunkUnsubscribed;
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

fn filter_chunk(chunk: &ChunkView, receiver_ids: &HashSet<AccountId>) -> Option<ChunkView> {
    if receiver_ids.is_empty() {
        return Some(ChunkView {
            author: chunk.author.clone(),
            header: chunk.header.clone(),
            transactions: chunk.transactions.clone(),
            receipts: chunk.receipts.clone(),
        });
    }

    let transactions: Vec<_> = chunk
        .transactions
        .iter()
        .filter(|tx| receiver_ids.contains(&tx.receiver_id))
        .cloned()
        .collect();

    let receipts: Vec<_> = chunk
        .receipts
        .iter()
        .filter(|rcpt| receiver_ids.contains(&rcpt.receiver_id))
        .cloned()
        .collect();

    if transactions.is_empty() && receipts.is_empty() {
        return None;
    }

    Some(ChunkView {
        author: chunk.author.clone(),
        header: chunk.header.clone(),
        transactions,
        receipts,
    })
}

async fn send_error(sender: &mut (impl Sink<Message> + Unpin), message: &'static str) {
    if let Ok(json) = serde_json::to_string(&WsResponse::Error { message }) {
        let _ = sender.send(Message::Text(json.into())).await;
    }
}
