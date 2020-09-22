use crate::publisher::Publisher;
use async_trait::async_trait;
use cord_message::Message;
use futures::Future;
use log::debug;
use tokio::sync::mpsc::UnboundedSender;

#[async_trait]
pub trait Subscriber {
    async fn recv(&mut self, publisher: &Publisher, message: Message) -> bool;
}

/// A Subscriber that receives a stream of messages
pub struct Consumer(pub UnboundedSender<Message>);

/// A Subscriber that executes a task for each message
pub struct Task<F, T>(pub T)
where
    F: Future + Send,
    T: (FnMut(&Publisher, Message) -> F) + Send;

/// A Subscriber that executes a task for a single message
pub struct OnetimeTask<F, T>(pub Option<T>)
where
    F: Future + Send,
    T: (FnOnce(&Publisher, Message) -> F) + Send;

#[async_trait]
impl Subscriber for Consumer {
    async fn recv(&mut self, _: &Publisher, message: Message) -> bool {
        debug!(target: "subscriber", "Receive message for consumer: {:?}", message);
        self.0.send(message).is_ok()
    }
}

#[async_trait]
impl<F, T> Subscriber for Task<F, T>
where
    F: Future + Send,
    T: (FnMut(&Publisher, Message) -> F) + Send,
{
    async fn recv(&mut self, publisher: &Publisher, message: Message) -> bool {
        debug!(target: "subscriber", "Receive message for task: {:?}", message);
        self.0(publisher, message).await;
        true // Retain subscriber in map
    }
}

#[async_trait]
impl<F, T> Subscriber for OnetimeTask<F, T>
where
    F: Future + Send,
    T: (FnOnce(&Publisher, Message) -> F) + Send,
{
    async fn recv(&mut self, publisher: &Publisher, message: Message) -> bool {
        debug!(target: "subscriber", "Receive message for one-time task: {:?}", message);
        self.0.take().expect("OnetimeTask already executed")(publisher, message).await;
        false // Don't retain subscriber in map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn test_subscriber_recv_consumer() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let publisher = Publisher::new("localhost:1234".parse().unwrap(), tx.clone());
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = Consumer(tx);
        let retain = consumer.recv(&publisher, message).await;
        assert!(retain);

        assert_eq!(rx.recv().await, Some(message_c));
    }

    #[tokio::test]
    async fn test_subscriber_recv_task() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let publisher = Publisher::new("localhost:1234".parse().unwrap(), tx.clone());
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = Task(|_, msg| async {
            tx.send(msg).unwrap();
        });
        let retain = consumer.recv(&publisher, message).await;
        assert!(retain);

        assert_eq!(rx.recv().await, Some(message_c));
    }

    #[tokio::test]
    async fn test_subscriber_recv_onetime_task() {
        let (tx, rx) = oneshot::channel();
        let (txu, _) = mpsc::unbounded_channel();
        let publisher = Publisher::new("localhost:1234".parse().unwrap(), txu);
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = OnetimeTask(Some(|_: &_, msg| async {
            tx.send(msg).unwrap();
        }));
        consumer.recv(&publisher, message).await;

        assert_eq!(rx.await, Ok(message_c));
    }
}
