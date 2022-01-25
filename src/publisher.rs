use crate::subscriber::{Consumer, OnetimeTask, Subscriber, Task};
use cord_message::{Message, Pattern};
use futures::{self, future, Future, FutureExt};
use futures_locks::Mutex;
use log::debug;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
    net::SocketAddr,
};

#[derive(Clone)]
pub struct Publisher {
    name: SocketAddr,
    // This is a channel to the write half of our TcpStream
    consumer: UnboundedSender<Message>,
    inner: Mutex<PublishInner>,
}

pub struct PublishInner {
    provides: Vec<Pattern>,
    subscribes: Vec<Pattern>,
    subscribers: HashMap<Message, HashMap<Uuid, Box<dyn Subscriber + Send>>>,
}

impl fmt::Display for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Publisher<{}>", self.name)
    }
}

impl Publisher {
    pub fn new(name: SocketAddr, consumer: UnboundedSender<Message>) -> Publisher {
        Publisher {
            name,
            consumer,
            inner: Mutex::new(PublishInner {
                provides: Vec::new(),
                subscribes: Vec::new(),
                subscribers: HashMap::new(),
            }),
        }
    }

    // Link two publishers together so that they can subscribe to each other's streams
    pub async fn link(&self, other: &Publisher) {
        debug!(target: "publisher", "Link {} with {}", self, other);

        // Link publishers
        future::join(self.on_link(other), other.on_link(self)).await;

        // Replay PROVIDE messages for new joiner
        let mut futs = Vec::new();
        let guard = self.inner.lock().await;
        (*guard).provides.iter().for_each(|p| {
            debug!(target: "publisher", "Replay {:?} for new joiner", p);
            futs.push(other.on_provide(p.clone(), &self));
        });
        future::join_all(futs).await;
    }

    // Helper function for subscribing another publisher to our PROVIDE and REVOKE
    // messages
    async fn on_link(&self, other: &Publisher) {
        // Subscribe to PROVIDE messages
        let other_c = other.clone();
        let self_c = self.clone();
        self.subscribe(
            Message::Provide("/".into()),
            Task(move |message| other_c.on_provide(message.unwrap_provide(), &self_c)),
        )
        .await;

        // Subscribe to REVOKE messages
        // We use this task to bulk-remove all subscribers that have subscribed to the
        // revoked namespace.
        let self_c = self.clone();
        self.subscribe(
            Message::Revoke("/".into()),
            Task(move |message| {
                self_c.unsubscribe_children(Message::Event(message.unwrap_revoke(), String::new()))
            }),
        )
        .await;
    }

    // Helper function for subscribing another publisher to our SUBSCRIBE messages
    fn on_provide(&self, namespace: Pattern, other: &Publisher) -> impl Future<Output = ()> {
        // Add a subscription for any SUBSCRIBE messages from our client that match the
        // other publisher's namespace. I.e. if our client wants to subscribe to /a and
        // the other publisher provides /a, forward our client's request to the other
        // publisher. The other publisher will then start sending /a messages to our
        // client.
        let self_c = self.clone();
        let other_c = other.clone();
        let namespace_c = namespace.clone();
        let sub_fut = self.subscribe(
            Message::Subscribe(namespace_c),
            Task(move |message| other_c.on_subscribe(message.unwrap_subscribe(), self_c.clone())),
        );

        // Add a subscription for any REVOKE messages from the other publisher. If the
        // other provider revokes the namespace, we don't want to keep listening for
        // SUBSCRIBE messages to it.
        let self_c = self.clone();
        let other_c = other.clone();
        let namespace_c = namespace.clone();
        let rev_fut = sub_fut.then(move |uuid| {
            other_c.subscribe(
                Message::Revoke(namespace_c),
                OnetimeTask(Some(move |message: Message| {
                    self_c.unsubscribe(Message::Subscribe(message.unwrap_revoke()), uuid)
                })),
            )
        });

        // Replay SUBSCRIBE messages received prior to this PROVIDE
        let self_c = self.clone();
        let other_c = other.clone();
        rev_fut.then(move |_| {
            let self_c2 = self_c.clone();
            self_c.inner.with(move |guard| {
                let mut futs = Vec::new();
                (*guard)
                    .subscribes
                    .iter()
                    .filter(|s| namespace.contains(s))
                    .for_each(|s| {
                        debug!(target: "publisher", "Replay {:?} for new provide", s);
                        futs.push(other_c.on_subscribe(s.clone(), self_c2.clone()));
                    });
                future::join_all(futs).map(|_| ())
            })
        })
    }

    // Helper function for subscribing another publisher's consumer to our EVENT messages
    fn on_subscribe(&self, namespace: Pattern, other: Publisher) -> impl Future<Output = ()> {
        let me = self.clone();
        let namespace_clone = namespace.clone();
        let consumer = other.consumer.clone();

        // Subscribe a consumer to our EVENT messages matching `namespace`
        let sub_fut = self.subscribe(Message::Event(namespace, String::new()), Consumer(consumer));

        // Add a subscription for any UNSUBSCRIBE messages from the other publisher. If
        // the other publisher receives a request to unsubscribe this namespace, the task
        // below will trigger it.
        sub_fut
            .then(move |uuid| {
                other.subscribe(
                    Message::Unsubscribe(namespace_clone),
                    OnetimeTask(Some(move |message: Message| {
                        me.unsubscribe(
                            Message::Event(message.unwrap_unsubscribe(), String::new()),
                            uuid,
                        )
                    })),
                )
            })
            .map(|_| ())
    }

    // Add a Subscriber for a specific Message to our subscriber list
    fn subscribe<S>(&self, message: Message, subscriber: S) -> impl Future<Output = Uuid>
    where
        S: Subscriber + Send + 'static,
    {
        let uuid = Uuid::new_v4();

        debug!(target: "publisher", "Subscribe {} to {:?} for {}", uuid, message, self);

        self.inner
            .with(move |mut guard| {
                (*guard)
                    .subscribers
                    .entry(message)
                    .or_insert_with(HashMap::new)
                    .insert(uuid, Box::new(subscriber));
                future::ready(())
            })
            .map(move |_| uuid)
    }

    // Remove a Subscriber for a specific Message from our subscriber list
    fn unsubscribe(&self, message: Message, sub_id: Uuid) -> impl Future<Output = ()> {
        debug!(target: "publisher", "Unsubscribe {} from {:?} for {}", sub_id, message, self);

        self.inner.with(move |mut guard| {
            if let Entry::Occupied(mut o) = (*guard).subscribers.entry(message) {
                let e = o.get_mut();
                e.remove(&sub_id);

                // If that was the last subscriber for a particular message, delete
                // the message so it doesn't add more overhead to the routing loop.
                if e.is_empty() {
                    o.remove();
                }
            }
            future::ready(())
        })
    }

    // Remove all Subscribers that are are contained within a broader namespace.
    // For example: Subscribe(/a) contains Subscribe(/a/b), so Subscribe(/a/b) would be
    // removed.
    fn unsubscribe_children(&self, message: Message) -> impl Future<Output = ()> {
        debug!(target: "publisher", "Unsubscribe everyone from {:?} for {}", message, self);

        self.inner.with(move |mut guard| {
            (*guard).subscribers.retain(|k, _| !message.contains(&k));
            future::ready(())
        })
    }

    // Route a message to all interested subscribers, making sure we clean up any
    // subscribers that are only executed once (i.e. OnetimeTask).
    pub async fn route(&mut self, message: Message) {
        let name = self.name;

        self.inner
            .with(move |mut guard| {
                // Cache important messages so we can replay them to new joiners
                match message {
                    Message::Provide(ref pattern) => (*guard).provides.push(pattern.clone()),
                    Message::Revoke(ref pattern) => {
                        (*guard).provides.retain(|provide| provide != pattern)
                    }
                    Message::Subscribe(ref pattern) => (*guard).subscribes.push(pattern.clone()),
                    Message::Unsubscribe(ref pattern) => {
                        (*guard).subscribes.retain(|subscribe| subscribe != pattern)
                    }
                    _ => (),
                }

                debug!("Received message {:?} from {}", message, name);

                let mut futs = vec![];

                // In the inner loop we may cleanup all of the subscribers in the
                // `subs` map. This leaves us with an empty map that should also
                // be tidied up, hence the use of retain() here.
                (*guard).subscribers.retain(|sub_msg, subs| {
                    if sub_msg.contains(&message) {
                        debug!("Subscriber {:?} matches {:?}", sub_msg, message);
                        // We use retain() here, which allows us to cleanup any
                        // subscribers that have OnetimeTask's or whose channels
                        // have been closed.
                        subs.retain(|_, sub| {
                            let (retain, f) = sub.recv(message.clone());
                            futs.push(f);
                            retain
                        });
                        !subs.is_empty() // Retain if map contains subscribers
                    } else {
                        debug!("Subscriber {:?} does not match {:?}", sub_msg, message);
                        true // Retain as we haven't modified the map
                    }
                });

                future::join_all(futs).map(|_| ())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::{self, ready};
    use tokio::sync::{
        mpsc,
        oneshot::{self, error::TryRecvError},
    };

    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    async fn create_publisher(
        subscribers: Option<HashMap<Message, HashMap<Uuid, Box<dyn Subscriber + Send>>>>,
        subscribes: Option<Vec<Pattern>>,
    ) -> Publisher {
        // Instantiate objects for testing
        let (tx, _) = mpsc::unbounded_channel();
        let publisher = Publisher::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7100),
            tx,
        );

        // Add subscribers to publisher
        publisher
            .inner
            .with(move |mut guard| {
                if let Some(subs) = subscribers {
                    (*guard).subscribers = subs;
                }

                if let Some(subs) = subscribes {
                    (*guard).subscribes = subs;
                }

                future::ready(())
            })
            .await;

        publisher
    }

    fn add_subscriber_consumer(
        subscribers: &mut HashMap<Uuid, Box<dyn Subscriber + Send>>,
    ) -> Uuid {
        let uuid = Uuid::new_v4();

        let (tx, _) = mpsc::unbounded_channel();
        let subscriber = Consumer(tx);

        subscribers.insert(uuid, Box::new(subscriber));

        uuid
    }

    fn add_subscriber_task<F, Fut>(
        subscribers: &mut HashMap<Uuid, Box<dyn Subscriber + Send>>,
        function: F,
    ) -> Uuid
    where
        Fut: Future<Output = ()> + Send + 'static,
        F: (FnMut(Message) -> Fut) + Send + 'static,
    {
        let uuid = Uuid::new_v4();

        let subscriber = Task(function);

        subscribers.insert(uuid, Box::new(subscriber));

        uuid
    }

    fn add_subscriber_onetime<F, Fut>(
        subscribers: &mut HashMap<Uuid, Box<dyn Subscriber + Send>>,
        function: F,
    ) -> Uuid
    where
        Fut: Future<Output = ()> + Send + 'static,
        F: (FnOnce(Message) -> Fut) + Send + 'static,
    {
        let uuid = Uuid::new_v4();

        let subscriber = OnetimeTask(Some(function));

        subscribers.insert(uuid, Box::new(subscriber));

        uuid
    }

    #[tokio::test]
    async fn test_on_link() {
        let publisher1 = create_publisher(None, None).await;
        let publisher2 = create_publisher(None, None).await;

        publisher1.on_link(&publisher2).await;

        let inner = publisher1.inner.try_lock().ok().unwrap();

        let m1 = Message::Provide("/".into());
        assert_eq!(inner.subscribers[&m1].len(), 1);

        let m2 = Message::Revoke("/".into());
        assert_eq!(inner.subscribers[&m2].len(), 1);
    }

    #[tokio::test]
    async fn test_on_provide() {
        // Instantiate publishers for testing
        let publisher1 = create_publisher(None, Some(vec!["/a".into()])).await;
        let publisher2 = create_publisher(None, None).await;

        publisher1.on_provide("/a".into(), &publisher2).await;

        let inner1 = publisher1.inner.try_lock().ok().unwrap();
        let inner2 = publisher2.inner.try_lock().ok().unwrap();

        // Check that on_provide subscribed to subscribe messages
        let sub_msg = Message::Subscribe("/a".into());
        assert_eq!(inner1.subscribers[&sub_msg].len(), 1);

        // Check that on_provide subscribed to revoke messages
        let revoke_msg = Message::Revoke("/a".into());
        assert_eq!(inner2.subscribers[&revoke_msg].len(), 1);

        // Check that on_provide replayed subscribes
        let unsub_msg = Message::Unsubscribe("/a".into());
        assert_eq!(inner1.subscribers[&unsub_msg].len(), 1);

        // Check that on_subscribe subscribed to event messages
        let event_msg = Message::Event("/a".into(), String::new());
        assert_eq!(inner2.subscribers[&event_msg].len(), 1);
    }

    #[tokio::test]
    async fn test_on_subscribe() {
        // Instantiate publishers for testing
        let publisher1 = create_publisher(None, None).await;
        let publisher2 = create_publisher(None, None).await;
        let publisher2_handle = publisher2.clone();

        publisher1.on_subscribe("/a".into(), publisher2).await;

        // Check that on_subscribe subscribed to unsubscribe messages
        let unsub_msg = Message::Unsubscribe("/a".into());
        let inner = publisher2_handle.inner.try_lock().ok().unwrap();
        assert_eq!(inner.subscribers[&unsub_msg].len(), 1);

        // Check that on_subscribe subscribed to event messages
        let event_msg = Message::Event("/a".into(), String::new());
        let inner = publisher1.inner.try_lock().ok().unwrap();
        assert_eq!(inner.subscribers[&event_msg].len(), 1);
    }

    #[tokio::test]
    async fn test_subscribe() {
        // Instantiate objects for testing
        let (tx, _) = mpsc::unbounded_channel();
        let subscriber = Consumer(tx.clone());
        let publisher = create_publisher(None, None).await;
        let message = Message::Provide("/a".into());

        let uuid = publisher.subscribe(message.clone(), subscriber).await;

        // Check that subscribe created a new subscribers entry
        let inner = publisher.inner.try_lock().ok().unwrap();
        assert!(inner.subscribers[&message].contains_key(&uuid));
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        // Create message to unsubscribe from
        let message = Message::Provide("/a".into());

        // Setup subscriber for testing
        let mut sub = HashMap::new();
        let uuid = add_subscriber_consumer(&mut sub);

        // Setup subscribers hash
        let mut subs = HashMap::new();
        subs.insert(message.clone(), sub);

        let publisher = create_publisher(Some(subs), None).await;
        publisher.unsubscribe(message.clone(), uuid).await;

        // Check that unsubscribe removed the subscribers entry
        let inner = publisher.inner.try_lock().ok().unwrap();
        assert!(!inner.subscribers.contains_key(&message));
    }

    #[tokio::test]
    async fn test_unsubscribe_multiple_subs() {
        // Create message to unsubscribe from
        let message = Message::Provide("/a".into());

        // Setup subscriber for testing
        let mut sub = HashMap::new();
        let uuid1 = add_subscriber_consumer(&mut sub);
        let uuid2 = add_subscriber_consumer(&mut sub);

        // Setup subscribers hash
        let mut subs = HashMap::new();
        subs.insert(message.clone(), sub);

        let publisher = create_publisher(Some(subs), None).await;
        publisher.unsubscribe(message.clone(), uuid1).await;

        // Check that unsubscribe removed the subscribers entry
        let inner = publisher.inner.try_lock().ok().unwrap();
        assert!(inner.subscribers.contains_key(&message));
        assert!(!inner.subscribers[&message].contains_key(&uuid1));
        assert!(inner.subscribers[&message].contains_key(&uuid2));
    }

    #[tokio::test]
    async fn test_unsubscribe_children() {
        // Create messages to unsubscribe from
        let message1 = Message::Provide("/a".into());
        let message2 = Message::Provide("/b".into());

        // Setup subscribers for testing
        let mut sub1 = HashMap::new();
        add_subscriber_consumer(&mut sub1);
        let mut sub2 = HashMap::new();
        add_subscriber_consumer(&mut sub2);

        // Setup subscribers hash
        let mut subs = HashMap::new();
        subs.insert(message1.clone(), sub1);
        subs.insert(message2.clone(), sub2);

        let publisher = create_publisher(Some(subs), None).await;
        publisher.unsubscribe_children(message1.clone()).await;

        // Check that unsubscribe removed the subscribers entry
        let inner = publisher.inner.try_lock().ok().unwrap();
        assert!(!inner.subscribers.contains_key(&message1));
        assert!(inner.subscribers.contains_key(&message2));
    }

    #[tokio::test]
    async fn test_route_none() {
        // Setup subscriber for testing
        let (tx, mut rx) = oneshot::channel();
        let mut sub = HashMap::new();
        let uuid = add_subscriber_onetime(&mut sub, |msg| async { tx.send(msg).unwrap() });

        // Setup subscribers hash
        let message = Message::Subscribe("/a".into());
        let mut subs = HashMap::new();
        subs.insert(message.clone(), sub);

        let mut publisher = create_publisher(Some(subs), None).await;
        publisher.route(Message::Provide("/b".into())).await;

        // Check that the subscriber was not triggered
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        // Check that subscribers contains the message we expect to have retained
        let inner = publisher.inner.try_lock().ok().unwrap();
        assert_eq!(inner.subscribers[&message].len(), 1);
        assert!(inner.subscribers[&message].contains_key(&uuid));
    }

    #[tokio::test]
    async fn test_route_retain() {
        // Setup subscriber for testing
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut sub = HashMap::new();
        let uuid = add_subscriber_task(&mut sub, move |msg| {
            tx.send(msg).unwrap();
            ready(())
        });

        // Setup subscribers hash
        let message = Message::Subscribe("/a".into());
        let mut subs = HashMap::new();
        subs.insert(message.clone(), sub);

        let mut publisher = create_publisher(Some(subs), None).await;
        publisher.route(message.clone()).await;

        // Check that the subscriber was triggered
        assert_eq!(rx.recv().await, Some(message.clone()));

        // Check that subscribers contains the message we expect to have retained
        let inner = publisher.inner.try_lock().ok().unwrap();
        assert_eq!(inner.subscribers[&message].len(), 1);
        assert!(inner.subscribers[&message].contains_key(&uuid));
    }

    #[tokio::test]
    async fn test_route_delete() {
        // Setup subscriber for testing
        let (tx, rx) = oneshot::channel();
        let mut sub = HashMap::new();
        add_subscriber_onetime(&mut sub, |msg| async { tx.send(msg).unwrap() });

        // Setup subscribers hash
        let message = Message::Subscribe("/a".into());
        let mut subs = HashMap::new();
        subs.insert(message.clone(), sub);

        let mut publisher = create_publisher(Some(subs), None).await;
        publisher.route(message.clone()).await;

        // Check that the subscriber was not triggered
        assert_eq!(rx.await, Ok(message.clone()));

        // Check that subscribers contains the message we expect to have retained
        let inner = publisher.inner.try_lock().ok().unwrap();
        assert!(!inner.subscribers.contains_key(&message));
    }
}
