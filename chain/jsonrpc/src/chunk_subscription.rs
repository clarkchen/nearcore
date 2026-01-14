#![allow(dead_code)]
//! Chunk subscription hub for WebSocket clients.
//!
//! Provides a lightweight publish-subscribe mechanism to stream chunk updates
//! to connected WebSocket clients.

use near_primitives::views::ChunkView;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;

/// Chunk subscription hub that manages WebSocket subscriptions.
#[derive(Clone)]
pub struct ChunkSubscriptionHub {
    /// Broadcast sender for distributing chunks to all subscribers.
    sender: broadcast::Sender<Arc<ChunkView>>,
    /// Current number of active subscribers (for metrics).
    subscriber_count: Arc<AtomicUsize>,
}

impl ChunkSubscriptionHub {
    /// Creates a new ChunkSubscriptionHub with the specified channel capacity.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of chunks to buffer. Slow receivers will
    ///                miss chunks if they fall behind by more than this amount.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender, subscriber_count: Arc::new(AtomicUsize::new(0)) }
    }

    /// Subscribe to receive new chunks.
    ///
    /// Returns a receiver that will receive all chunks published after subscription.
    pub fn subscribe(&self) -> ChunkSubscriber {
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            target: "jsonrpc",
            subscriber_count = self.subscriber_count.load(Ordering::Relaxed),
            "New chunk subscriber"
        );
        ChunkSubscriber { receiver: self.sender.subscribe(), hub: self.clone() }
    }

    /// Publish a new chunk to all subscribers.
    ///
    /// This is a non-blocking operation. If there are no subscribers,
    /// the chunk is simply discarded.
    pub fn publish(&self, chunk: ChunkView) {
        if self.subscriber_count.load(Ordering::Relaxed) == 0 {
            return;
        }

        let chunk = Arc::new(chunk);
        let _ = self.sender.send(chunk);
    }

    /// Get the current number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Relaxed)
    }

    fn unsubscribe(&self) {
        let prev = self.subscriber_count.fetch_sub(1, Ordering::Relaxed);
        tracing::info!(
            target: "jsonrpc",
            subscriber_count = prev.saturating_sub(1),
            "Chunk subscriber disconnected"
        );
    }
}

impl Default for ChunkSubscriptionHub {
    fn default() -> Self {
        // Buffer up to 256 chunks - enough to handle temporary slowdowns.
        Self::new(256)
    }
}

/// A subscriber handle that receives chunk updates.
///
/// When dropped, automatically decrements the subscriber count.
pub struct ChunkSubscriber {
    receiver: broadcast::Receiver<Arc<ChunkView>>,
    hub: ChunkSubscriptionHub,
}

impl ChunkSubscriber {
    /// Receive the next chunk.
    ///
    /// Returns None if the channel is closed (hub dropped).
    /// Chunks that were missed due to slow processing are skipped.
    pub async fn recv(&mut self) -> Option<Arc<ChunkView>> {
        loop {
            match self.receiver.recv().await {
                Ok(chunk) => return Some(chunk),
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(
                        target: "jsonrpc",
                        skipped = count,
                        "Subscriber lagged behind, skipping chunks"
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

impl Drop for ChunkSubscriber {
    fn drop(&mut self) {
        self.hub.unsubscribe();
    }
}
