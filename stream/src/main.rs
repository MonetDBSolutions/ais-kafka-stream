use std::io;
use std::time::Duration;
use clap::Parser;
use kafka::producer::{Producer, Record, RequiredAcks};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    hostname: String,

    #[clap(short, long, default_value_t = 9092)]
    port: i32,

    #[clap(short, long)]
    topic: String,

    #[clap(short, long)]
    silent: bool
}

fn main() {
    let args = Args::parse();

    let mut producer = get_producer(args.hostname, args.port).unwrap();

    loop {
        let mut buffer = String::new();
        match io::stdin().read_line(&mut buffer) {
            Ok(_) => (),
            Err(e) => println!("{}", e)
        }
        
        producer.send(&Record::from_value(&args.topic, buffer.as_bytes())).unwrap();

        if !args.silent {
            println!("{}", buffer);
        }
    }
}


fn get_producer(hostname: String, port: i32) -> Option<Producer> {
    let host = format!("{}:{}", hostname, port.to_string());
    
    let get_producer =
    Producer::from_hosts(vec!(host.to_owned()))
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();


    Some(get_producer)
}
