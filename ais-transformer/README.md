# AIS decoder/transformer

Java program that can decode raw AIS messages

## Usage:
This program can decode raw AIS messages either from (a) file(s) or from a kafka cluster.

## Flags
| Flag (long) | Short | Needs Value?                            | Command   |
|-------------|-------|-----------------------------------------|-----------|
| Kafka       | k     | No                                      | Use kafka |
| Files       | f     | Yes (comma seperated list of filenames) | Use files |

The following flags are mutually exclusive and are ignored by the other setting:

### Flags only applicable to Kafka
| Flag (long) | Short | Type   | Needs Value?               | Command                     |
|-------------|-------|--------|----------------------------|-----------------------------|
| host        | h     | String | No (defaults to localhost) | Set the kafka hostname      |
| port        | p     | Int    | No (defaults to 9092)      | Set the kafka hostname port |
| topic       | t     | String | No (defaults to test)      | Set the kafka topic name    |
| interval    | i     | Int    | No (defaults to 10)        | Set the polling interval    |


Example: 
```bash
java -jar ais-transformer-1.0.jar --kafka --host localhost --port 9092 --topic test --interval 10
```