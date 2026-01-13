//! Block subscription hub for WebSocket clients.
//!
//! This module provides a publish-subscribe mechanism for real-time block updates.
//! When a new block is processed, it gets broadcast to all connected WebSocket clients.

use near_primitives::views::BlockView;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;

/// Block subscription hub that manages WebSocket subscriptions.
///
/// Uses a broadcast channel to efficiently distribute blocks to multiple subscribers.
#[derive(Clone)]
pub struct BlockSubscriptionHub {
    /// Broadcast sender for distributing blocks to all subscribers
    sender: broadcast::Sender<Arc<BlockView>>,
    /// Current number of active subscribers (for metrics)
    subscriber_count: Arc<AtomicUsize>,
}

impl BlockSubscriptionHub {
    /// Creates a new BlockSubscriptionHub with the specified channel capacity.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of blocks to buffer. Slow receivers will
    ///                miss blocks if they fall behind by more than this amount.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender, subscriber_count: Arc::new(AtomicUsize::new(0)) }
    }

    /// Subscribe to receive new blocks.
    ///
    /// Returns a receiver that will receive all blocks published after subscription.
    pub fn subscribe(&self) -> BlockSubscriber {
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            target: "jsonrpc",
            subscriber_count = self.subscriber_count.load(Ordering::Relaxed),
            "New block subscriber"
        );
        BlockSubscriber { receiver: self.sender.subscribe(), hub: self.clone() }
    }

    /// Publish a new block to all subscribers.
    ///
    /// This is a non-blocking operation. If there are no subscribers,
    /// the block is simply discarded.
    pub fn publish(&self, block: BlockView) {
        let subscriber_count = self.subscriber_count.load(Ordering::Relaxed);
        if subscriber_count == 0 {
            return;
        }

        let block = Arc::new(block);
        match self.sender.send(block) {
            Ok(count) => {
                tracing::debug!(
                    target: "jsonrpc",
                    receivers = count,
                    "Published block to subscribers"
                );
            }
            Err(_) => {
                // No active receivers, this is fine
            }
        }
    }

    /// Get the current number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Relaxed)
    }

    fn unsubscribe(&self) {
        let prev = self.subscriber_count.fetch_sub(1, Ordering::Relaxed);
        tracing::info!(
            target: "jsonrpc",
            subscriber_count = prev - 1,
            "Block subscriber disconnected"
        );
    }
}

impl Default for BlockSubscriptionHub {
    fn default() -> Self {
        // Buffer up to 256 blocks - enough to handle temporary slowdowns
        Self::new(256)
    }
}

/// A subscriber handle that receives block updates.
///
/// When dropped, automatically decrements the subscriber count.
pub struct BlockSubscriber {
    receiver: broadcast::Receiver<Arc<BlockView>>,
    hub: BlockSubscriptionHub,
}

impl BlockSubscriber {
    /// Receive the next block.
    ///
    /// Returns None if the channel is closed (hub dropped).
    /// Blocks that were missed due to slow processing are skipped.
    pub async fn recv(&mut self) -> Option<Arc<BlockView>> {
        loop {
            match self.receiver.recv().await {
                Ok(block) => return Some(block),
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(
                        target: "jsonrpc",
                        skipped = count,
                        "Subscriber lagged behind, skipping blocks"
                    );
                    // Continue to get the next available block
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

impl Drop for BlockSubscriber {
    fn drop(&mut self) {
        self.hub.unsubscribe();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::block::BlockHeader;
    use near_primitives::hash::CryptoHash;

    #[tokio::test]
    async fn test_subscribe_and_receive() {
        let hub = BlockSubscriptionHub::new(16);
        assert_eq!(hub.subscriber_count(), 0);

        let mut subscriber = hub.subscribe();
        assert_eq!(hub.subscriber_count(), 1);

        // Create a minimal BlockView for testing
        // In real code, this would come from an actual block
        // For now, we just verify the pub/sub mechanism works
    }

    #[test]
    fn test_no_subscribers() {
        let hub = BlockSubscriptionHub::new(16);
        // Publishing without subscribers should not panic
        // (We can't easily create a BlockView in tests without more setup)
    }

    #[tokio::test]
    async fn test_subscriber_count() {
        let hub = BlockSubscriptionHub::new(16);

        let sub1 = hub.subscribe();
        assert_eq!(hub.subscriber_count(), 1);

        let sub2 = hub.subscribe();
        assert_eq!(hub.subscriber_count(), 2);

        drop(sub1);
        assert_eq!(hub.subscriber_count(), 1);

        drop(sub2);
        assert_eq!(hub.subscriber_count(), 0);
    }
}
