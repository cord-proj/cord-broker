#[allow(deprecated)]
mod errors;
mod publisher;
mod subscriber;

use clap::{crate_authors, crate_description, crate_name, crate_version, App, Arg};
use cord_message::{Codec, Message};
use env_logger;
use errors::*;
use futures::{
    future::{self, FutureExt, TryFutureExt},
    stream::{SplitSink, StreamExt, TryStreamExt},
};
use log::error;
use publisher::Publisher;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedSender},
};
use tokio_util::codec::Framed;

use std::net::{IpAddr, SocketAddr};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let matches = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .arg(
            Arg::with_name("bind")
                .short("a")
                .long("bind-address")
                .value_name("ADDRESS")
                .help("The IP address to bind this service to (e.g. 0.0.0.0 for all addresses) - defaults to 127.0.0.1")
                .takes_value(true)
                .default_value("127.0.0.1")
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("The port number to bind this service to - defaults to 7101")
                .takes_value(true)
                .default_value("7101")
        ).get_matches();

    // Bind the broker's socket
    // `unwrap()` is safe as a default value will always be available
    let port: u16 = matches
        .value_of("port")
        .unwrap()
        .trim()
        .parse()
        .expect("Port must be an integer");
    // `unwrap()` is safe as a default value will always be available
    let addr: IpAddr = matches.value_of("bind").unwrap().trim().parse().unwrap();
    let listener = TcpListener::bind((addr, port)).await?;

    // If port is set to 0, the user wants us to bind to a random port. It would be
    // neighbourly to tell them what we've bound to!
    if port == 0 {
        if let Ok(SocketAddr::V4(s)) = listener.local_addr() {
            println!("{}", s.port());
        }
    }

    // Create a new vector to store handles for all of the existing publishers
    let mut publishers: Vec<Publisher> = vec![];

    // Poll the socket for incoming connections
    loop {
        match listener.accept().await {
            Ok((sock, addr)) => {
                // Wrap socket in message codec
                let framed = Framed::new(sock, Codec::default());
                let (sink, stream) = framed.split();

                // Convert sink to channel so that we can clone it and distribute to
                // multiple publishers
                let subscriber = sink_to_channel(sink);

                // Create a clonable publisher handle so we can control the publisher from afar
                let mut handle = Publisher::new(addr, subscriber);
                let newbie = handle.clone();

                // Introduce the newbie to all the other publishers. This allows each
                // publisher to subscribe to the other publishers' SUBSCRIBE events. This
                // is important for facilitating subscriptions between consumers of
                // different publishers.
                let mut futs = Vec::new();
                for publisher in publishers.iter() {
                    futs.push(publisher.link(&newbie));
                }

                // This is the main routing task. For each message we receive, find all the
                // subscribers that match it, then pass each a copy via `recv()`.
                tokio::spawn(future::join_all(futs).then(|_| {
                    stream
                        // .map_err(|e| errors::ErrorKind::Message(e).into())
                        .try_fold(handle, |h, m| h.route(m).map(|_| Ok(h)))
                        .map_err(|e| error!("{}", e))
                }));

                // Finally, add the newbie to the list of existing publishers
                publishers.push(newbie);
            }
            Err(e) => error!("Could not accept incoming connection: {}", e),
        }
    }
}

// Create a channel for the consumer (sink) half of the socket. This allows us to
// pass clones of the channel to multiple producers to facilitate a consumer's
// subscriptions.
fn sink_to_channel(sink: SplitSink<Framed<TcpStream, Codec>, Message>) -> UnboundedSender<Message> {
    let (tx, rx) = mpsc::unbounded_channel();

    // Spawn task to drain channel receiver into socket sink
    tokio::spawn(rx.map(|m| Ok(m)).forward(sink));

    tx
}
