//! Transaction lifecycle subscription hub for WebSocket clients.
//!
//! This module provides a publish-subscribe mechanism for real-time transaction updates
//! at different stages of the transaction lifecycle: Pending, Confirmed, and Executed.

use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_primitives::views::{ActionView, ExecutionOutcomeWithIdView, SignedTransactionView};
use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Transaction lifecycle stages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStage {
    Pending,
    Confirmed,
    Executed,
}

/// Event emitted when a transaction enters the pending state (mempool)
#[derive(Debug, Clone, Serialize)]
pub struct PendingTxEvent {
    pub tx_hash: CryptoHash,
    pub signer_id: AccountId,
    pub receiver_id: AccountId,
    pub actions: Vec<ActionView>,
    pub nonce: u64,
    pub timestamp_ms: u64,
}

/// Event emitted when a transaction is confirmed in a chunk
#[derive(Debug, Clone, Serialize)]
pub struct ConfirmedTxEvent {
    pub tx_hash: CryptoHash,
    pub signer_id: AccountId,
    pub receiver_id: AccountId,
    pub shard_id: ShardId,
    pub height: BlockHeight,
    pub chunk_hash: CryptoHash,
    pub tx_index: usize,
    pub transaction: SignedTransactionView,
    pub timestamp_ms: u64,
}

/// Event emitted when a transaction is executed
#[derive(Debug, Clone, Serialize)]
pub struct ExecutedTxEvent {
    pub tx_hash: CryptoHash,
    pub signer_id: AccountId,
    pub receiver_id: AccountId,
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
    pub shard_id: ShardId,
    pub outcome: ExecutionOutcomeWithIdView,
    pub timestamp_ms: u64,
}

/// A transaction lifecycle event that can be any of the three stages
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "stage", content = "data")]
pub enum TransactionLifecycleEvent {
    #[serde(rename = "pending")]
    Pending(PendingTxEvent),
    #[serde(rename = "confirmed")]
    Confirmed(ConfirmedTxEvent),
    #[serde(rename = "executed")]
    Executed(ExecutedTxEvent),
}

impl TransactionLifecycleEvent {
    pub fn receiver_id(&self) -> &AccountId {
        match self {
            TransactionLifecycleEvent::Pending(e) => &e.receiver_id,
            TransactionLifecycleEvent::Confirmed(e) => &e.receiver_id,
            TransactionLifecycleEvent::Executed(e) => &e.receiver_id,
        }
    }

    pub fn tx_hash(&self) -> &CryptoHash {
        match self {
            TransactionLifecycleEvent::Pending(e) => &e.tx_hash,
            TransactionLifecycleEvent::Confirmed(e) => &e.tx_hash,
            TransactionLifecycleEvent::Executed(e) => &e.tx_hash,
        }
    }

    pub fn stage(&self) -> TransactionStage {
        match self {
            TransactionLifecycleEvent::Pending(_) => TransactionStage::Pending,
            TransactionLifecycleEvent::Confirmed(_) => TransactionStage::Confirmed,
            TransactionLifecycleEvent::Executed(_) => TransactionStage::Executed,
        }
    }
}

/// Transaction subscription hub that manages WebSocket subscriptions.
#[derive(Clone)]
pub struct TransactionSubscriptionHub {
    sender: broadcast::Sender<Arc<TransactionLifecycleEvent>>,
    subscriber_count: Arc<AtomicUsize>,
}

impl TransactionSubscriptionHub {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender, subscriber_count: Arc::new(AtomicUsize::new(0)) }
    }

    pub fn subscribe(&self) -> TransactionSubscriber {
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            target: "jsonrpc",
            subscriber_count = self.subscriber_count.load(Ordering::Relaxed),
            "New transaction subscriber"
        );
        TransactionSubscriber { receiver: self.sender.subscribe(), hub: self.clone() }
    }

    pub fn publish(&self, event: TransactionLifecycleEvent) {
        if self.subscriber_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let event = Arc::new(event);
        if let Ok(count) = self.sender.send(event) {
            tracing::trace!(
                target: "jsonrpc",
                receivers = count,
                "Published transaction event"
            );
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Relaxed)
    }

    fn unsubscribe(&self) {
        let prev = self.subscriber_count.fetch_sub(1, Ordering::Relaxed);
        tracing::info!(
            target: "jsonrpc",
            subscriber_count = prev - 1,
            "Transaction subscriber disconnected"
        );
    }
}

impl Default for TransactionSubscriptionHub {
    fn default() -> Self {
        Self::new(1024)
    }
}

/// A subscriber handle that receives transaction lifecycle events.
pub struct TransactionSubscriber {
    receiver: broadcast::Receiver<Arc<TransactionLifecycleEvent>>,
    hub: TransactionSubscriptionHub,
}

impl TransactionSubscriber {
    pub async fn recv(&mut self) -> Option<Arc<TransactionLifecycleEvent>> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(
                        target: "jsonrpc",
                        skipped = count,
                        "Transaction subscriber lagged"
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

impl Drop for TransactionSubscriber {
    fn drop(&mut self) {
        self.hub.unsubscribe();
    }
}
