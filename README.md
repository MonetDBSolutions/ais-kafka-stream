# AIS Kafka streaming test

Repository to showcase the MonetDB -> Kafka integration.

## Setting up Kafka
First, download Kafka.

Then start the server:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

and in a different terminal:
```bash
bin/kafka-server-start.sh config/server.properties
```

Then you can create a topic:
```bash
bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic test --bootstrap-server localhost:9092
```

To read the messages from a topic:
```bash
bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092
```

## Live streaming
Live streaming data is used by the 'stream' program. This is a program written in Rust that
catches the live data and puts it into a Kafka cluster.
