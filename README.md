# AIS Kafka streaming test

Repository to showcase the MonetDB -> Kafka integration.

## Live streaming
Live streaming data is used by the 'stream' program. This is a program written in Rust that
catches the live data and puts it into a Kafka cluster.

To run it:

```bash
cd stream
cargo build --release
cd /target/release
nc 153.44.253.27 5631 | ./rust-kafka-test
```
