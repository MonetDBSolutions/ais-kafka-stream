package com.monetdb.ais;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaToMonetDB {
    KafkaConsumer consumer;


    public KafkaToMonetDB(String host, int port, String topic) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
      
        consumer.subscribe(Arrays.asList(topic));

        this.consumer = consumer;
    }

    public List<String> getMessagesFromKafka() {
        List<String> values = new ArrayList<String>();
         ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
               values.add(record.value());
            }

            return values;
      }    

}

