//! WebSocket handler for real-time block subscriptions.
//!
//! This module provides WebSocket endpoints for clients to subscribe to
//! real-time block updates without polling.

use crate::block_subscription::BlockSubscriptionHub;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use near_primitives::views::BlockView;
use serde::{Deserialize, Serialize};
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
        block: &'a BlockView,
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
    pub hub: Arc<BlockSubscriptionHub>,
}

/// Axum handler for WebSocket upgrade
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.hub))
}

/// Handle an individual WebSocket connection
async fn handle_socket(socket: WebSocket, hub: Arc<BlockSubscriptionHub>) {
    let (mut sender, mut receiver) = socket.split();

    // Generate a unique subscription ID
    let subscription_id = format!("{:016x}", rand::random::<u64>());

    // Subscribe to block updates
    let mut block_subscriber = hub.subscribe();

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
