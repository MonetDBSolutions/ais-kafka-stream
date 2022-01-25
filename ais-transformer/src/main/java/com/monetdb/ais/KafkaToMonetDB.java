package com.monetdb.ais;

public class KafkaToMonetDB {
    private String host;    
    private int port;    
    private String topic;    

    public KafkaToMonetDB(String host, int port, String topic) {
        this.host = host;
        this.port = port;
        this.topic = topic;
    }

    public void getMessagesFromKafka() {
        System.out.println("getting message"); 
    }

}
