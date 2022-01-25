#[allow(deprecated)]
mod errors;
mod publisher;
mod subscriber;

use clap::Parser;
use cord_message::{Codec, Message};
use env_logger;
use errors::*;
use futures::{
    future::{self, TryFutureExt},
    stream::{SplitSink, StreamExt, TryStreamExt},
};
use log::error;
use publisher::Publisher;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedSender},
};
use tokio_util::codec::Framed;

use std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr};

// Parse string inputs into IP addresses
fn parse_ip(input: &str) -> ::std::result::Result<IpAddr, AddrParseError> {
    input.parse()
}

#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    /// The IP address to bind this service to (e.g. 0.0.0.0 for all addresses) - defaults to 127.0.0.1
    #[clap(short, long, default_value_t = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), parse(try_from_str = parse_ip))]
    address: IpAddr,

    /// The port number to bind this service to - defaults to 7101
    #[clap(short, long, default_value_t = 7101, parse(try_from_str))]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    // Bind the broker's socket
    let listener = TcpListener::bind((args.address, args.port)).await?;

    // If port is set to 0, the user wants us to bind to a random port. It would be
    // neighbourly to tell them what we've bound to!
    if args.port == 0 {
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
                let publisher = Publisher::new(addr, subscriber);

                // Introduce the newbie to all the other publishers. This allows each
                // publisher to subscribe to the other publishers' SUBSCRIBE events. This
                // is important for facilitating subscriptions between consumers of
                // different publishers.
                let mut futs = Vec::new();
                for p in publishers.iter() {
                    futs.push(p.link(&publisher));
                }
                future::join_all(futs).await;

                // This is the main routing task. For each message we receive, find all the
                // subscribers that match it, then pass each a copy via `recv()`.
                tokio::spawn(
                    stream
                        .try_fold(publisher.clone(), |mut p, m| async {
                            p.route(m).await;
                            Ok(p)
                        })
                        .map_err(|e| error!("{}", e)),
                );

                // Finally, add the newbie to the list of existing publishers
                publishers.push(publisher);
            }
            Err(e) => error!("Could not accept incoming connection: {}", e),
        }
    }
}

// Create a channel for the consumer (sink) half of the socket. This allows us to
// pass clones of the channel to multiple producers to facilitate a consumer's
// subscriptions.
fn sink_to_channel(sink: SplitSink<Framed<TcpStream, Codec>, Message>) -> UnboundedSender<Message> {
    let (tx, mut rx) = unbounded_channel();

    // Wrap the receiver in a Stream so we can forward it to a Sink
    let stream = async_stream::stream! {
        while let Some(item) = rx.recv().await {
            // Wrapping item in a Result to appease futures::StreamExt, which defaults to
            // a TryStream
            yield Ok(item);
        }
    };

    // Spawn task to drain channel receiver into socket sink
    tokio::spawn(stream.forward(sink));

    tx
}
