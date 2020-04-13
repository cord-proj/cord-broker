# Base image
FROM rust:alpine as builder
WORKDIR /usr/src/cord
COPY . .
ENV RUSTFLAGS="-C target-feature=-crt-static"
RUN apk update && \
    apk upgrade && \
    apk add --update alpine-sdk && \
    cargo install --path .

# Broker
FROM alpine
COPY --from=builder /usr/local/cargo/bin/cord-broker /usr/local/bin/cord-broker
RUN apk update && apk upgrade
ENTRYPOINT ["cord-broker", "--bind-address=0.0.0.0"]
