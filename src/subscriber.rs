use crate::errors::Result;
use cord_message::Message;
use futures::{self, Future};
use log::debug;
use tokio::sync::mpsc::UnboundedSender;

type SomeFuture = Box<dyn Future<Output = Result<()>> + Send + Unpin>;

pub enum Subscriber {
    Consumer(UnboundedSender<Message>),
    // @todo Replace trait object with impl Trait once stabilised:
    // https://github.com/rust-lang/rust/issues/63066
    // Task(Box<FnMut(Message, PublisherHandle) -> impl Future<Item=(), Error=()>>)
    Task(Box<dyn FnMut(Message) -> SomeFuture + Send>),
    OnetimeTask(Option<Box<dyn FnOnce(Message) -> SomeFuture + Send>>),
}

impl Subscriber {
    pub async fn recv(&mut self, message: Message) -> bool {
        match self {
            // Retain subscriber in map if the channel is ok
            Subscriber::Consumer(ref mut chan) => {
                debug!(target: "subscriber", "Receive message for consumer: {:?}", message);
                chan.send(message).is_ok()
            }
            Subscriber::Task(f) => {
                debug!(target: "subscriber", "Receive message for task: {:?}", message);
                f(message).await;
                true // Retain subscriber in map
            }
            Subscriber::OnetimeTask(opt) => {
                debug!(target: "subscriber", "Receive message for one-time task: {:?}", message);
                opt.take().expect("OnetimeTask already executed")(message).await;
                false // Don't retain subscriber in map
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn test_subscriber_recv_consumer() {
        let (tx, rx) = mpsc::unbounded_channel();
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = Subscriber::Consumer(tx);
        let retain = consumer.recv(message).await;
        assert!(retain);

        assert_eq!(rx.recv().await, Some(message_c));
    }

    #[tokio::test]
    async fn test_subscriber_recv_task() {
        let (tx, rx) = oneshot::channel();
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = Subscriber::Task(Box::new(move |msg| {
            tx.send(msg).unwrap();
            Box::new(future::ok(()))
        }));

        consumer.recv(message).await;

        assert_eq!(rx.await, Ok(message_c));
    }

    #[tokio::test]
    async fn test_subscriber_recv_onetime_task() {
        let (tx, mut rx) = oneshot::channel();
        let message = Message::Event("/a".into(), "abc".into());
        let message_c = message.clone();

        let mut consumer = Subscriber::OnetimeTask(Some(Box::new(move |msg| {
            tx.send(msg).unwrap();
            Box::new(future::ok(()))
        })));

        consumer.recv(message).await;

        assert_eq!(rx.await, Ok(message_c));
    }
}
