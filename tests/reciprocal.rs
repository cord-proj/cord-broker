mod utils;

use cord_client::{Client, Subscriber};
use cord_message::Pattern;
use futures::stream::StreamExt;
use tokio::sync::oneshot;

#[tokio::test]
async fn test_reciprocal() {
    let f = |port| async move {
        let (tx, mut rx1) = oneshot::channel();
        let mut client1 = Client::connect(("127.0.0.1", port)).await.unwrap();
        client1.provide("/users".into()).await.unwrap();

        let group_rx = client1.subscribe("/groups".into()).await.unwrap();

        client1
            .event("/users/add".into(), "Mark has joined")
            .await
            .unwrap();
        send_event(group_rx, tx).await;

        let (tx, mut rx2) = oneshot::channel();
        let mut client2 = Client::connect(("127.0.0.1", port)).await.unwrap();
        client2.provide("/groups".into()).await.unwrap();

        let user_rx = client2.subscribe("/users".into()).await.unwrap();
        client2
            .event("/groups/add".into(), "Admin group created")
            .await
            .unwrap();
        send_event(user_rx, tx).await;

        assert_eq!(
            rx1.try_recv().unwrap(),
            ("/groups/add".into(), "Admin group created".into())
        );

        assert_eq!(
            rx2.try_recv().unwrap(),
            ("/users/add".into(), "Mark has joined".into())
        );
    };

    utils::run_client(f).await;
}

async fn send_event(rx: Subscriber, tx: oneshot::Sender<(Pattern, String)>) {
    let result = rx.into_future().await.0;
    tx.send(result.unwrap()).unwrap();
}
