# Stream 
Stream live AIS data directly into Kafka

## Live streaming
Live streaming data is used by the 'stream' program. This is a program written in Rust that
catches the live data and puts it into a Kafka cluster.

## Flags
| Flag                            | Command                                                                               |
|---------------------------------|---------------------------------------------------------------------------------------|
| hostname (String)               | Set the host on which Kafka runs.                                                     |
| port (i32 (int))                | Set the port of the host on which Kafka runs.                                         |
| topic (string)                  | Specific topic of Kafka on which you wish to operate.                                 |
| silent (bool, false if omitted) | The program logs it's output to the console, set if you wish this not to be the case. |

example:

```bash
cargo build --release
cd /target/release
nc 153.44.253.27 5631 | ./rust-kafka-test --hostname localhost --port 9092 --topic test --silent
```