# Cord Broker

![CI Code Testing and Linting](https://github.com/cord-proj/cord-broker/workflows/CI%20Code%20Testing%20and%20Linting/badge.svg)
![CI Security Audit on Push](https://github.com/cord-proj/cord-broker/workflows/CI%20Security%20Audit%20on%20Push/badge.svg)

Cord is a data streaming platform for composing, aggregating and distributing arbitrary
streams. The Broker crate provides stream aggregation and distribution functionality for
the platform. It is the central component that all clients communicate with to exchange
messages.

## Usage

First, start a new Cord Broker:

**Docker**

    $ docker run -d -p 7101:7101 --rm cordproj/cord-broker:0

**Cargo**

    $ cargo install cord-broker
    $ cord-broker &

Next, use the [Cord Client](https://github.com/cord-proj/cord-client) to interact with
the Broker. The easiest way to get started is by using the Client CLI.

Subscribe to a namespace:

**Docker**

    $ docker run --rm cordproj/cord-client:0 -a <broker_addr> sub /names

**Cargo**

    $ cargo install cord-client
    $ cord-client sub /namespaces

Publish to this namespace:

**Docker**

    $ docker run -it --rm cordproj/cord-client:0 -a <broker_addr> pub /names
    Start typing to create an event, then press enter to send it to the broker.
    Use the format: NAMESPACE=VALUE

    /names/first=Daz

**Cargo**

    $ cord-client pub /names
    Start typing to create an event, then press enter to send it to the broker.
    Use the format: NAMESPACE=VALUE

    /names/first=Daz
