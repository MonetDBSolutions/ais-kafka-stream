use std::io;
use std::time::Duration;
use kafka::producer::{Producer, Record, RequiredAcks};

fn main() {
    let mut producer = get_producer().unwrap();

    loop {
        let mut buffer = String::new();
        match io::stdin().read_line(&mut buffer) {
            Ok(e) => (),
            Err(e) => println!("{}", e)
        }
        
        producer.send(&Record::from_value("test", buffer.as_bytes())).unwrap();

        println!("{}", buffer);
    }
}


fn get_producer() -> Option<Producer> {
    let get_producer =
    Producer::from_hosts(vec!("localhost:9092".to_owned()))
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();


    Some(get_producer)
}
