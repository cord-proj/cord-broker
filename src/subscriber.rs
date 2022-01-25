use cord_message::Message;
use futures::{future, Future};
use log::debug;
use std::pin::Pin;
use tokio::sync::mpsc::UnboundedSender;

type SomeFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

pub trait Subscriber {
    fn recv(&mut self, message: Message) -> (bool, SomeFuture);
}

/// A Subscriber that receives a stream of messages
pub struct Consumer(pub UnboundedSender<Message>);

/// A Subscriber that executes a task for each message
pub struct Task<F, T>(pub T)
where
    F: Future + Send,
    T: (FnMut(Message) -> F) + Send;

/// A Subscriber that executes a task for a single message
pub struct OnetimeTask<F, T>(pub Option<T>)
where
    F: Future + Send,
    T: (FnOnce(Message) -> F) + Send;

impl Subscriber for Consumer {
    fn recv(&mut self, message: Message) -> (bool, SomeFuture) {
        debug!(target: "subscriber", "Receive message for consumer: {:?}", message);
        (self.0.send(message).is_ok(), Box::pin(future::ready(())))
    }
}

impl<F, T> Subscriber for Task<F, T>
where
    F: Future<Output = ()> + Send + 'static,
    T: (FnMut(Message) -> F) + Send,
{
    fn recv(&mut self, message: Message) -> (bool, SomeFuture) {
        debug!(target: "subscriber", "Receive message for task: {:?}", message);
        // true = Retain subscriber in map
        (true, Box::pin(self.0(message)))
    }
}

impl<F, T> Subscriber for OnetimeTask<F, T>
where
    F: Future<Output = ()> + Send + 'static,
    T: (FnOnce(Message) -> F) + Send,
{
    fn recv(&mut self, message: Message) -> (bool, SomeFuture) {
        debug!(target: "subscriber", "Receive message for one-time task: {:?}", message);
        // false = Don't retain subscriber in map
        (
            false,
            Box::pin(self.0.take().expect("OnetimeTask already executed")(
                message,
            )),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn test_subscriber_recv_consumer() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = Consumer(tx);
        let (retain, f) = consumer.recv(message);
        assert!(retain);

        f.await;
        assert_eq!(rx.recv().await, Some(message_c));
    }

    #[tokio::test]
    async fn test_subscriber_recv_task() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = Task(|msg| {
            tx.send(msg).unwrap();
            future::ready(())
        });
        let (retain, f) = consumer.recv(message);
        assert!(retain);

        f.await;
        assert_eq!(rx.recv().await, Some(message_c));
    }

    #[tokio::test]
    async fn test_subscriber_recv_onetime_task() {
        let (tx, rx) = oneshot::channel();
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = OnetimeTask(Some(|msg| async {
            tx.send(msg).unwrap();
        }));
        let (_, f) = consumer.recv(message);

        f.await;
        assert_eq!(rx.await, Ok(message_c));
    }
}
