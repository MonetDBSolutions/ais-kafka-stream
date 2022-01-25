package com.monetdb.ais;

import java.util.*;
import java.nio.file.Paths;

import dk.dma.ais.message.AisMessage;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class TransformerMain {
    public static void main( String args[] ) {
        Parser parser = new Parser();

        Options opts = createOptions();
        CommandLineParser cmdParser = new DefaultParser();

        try {
            CommandLine line = cmdParser.parse(opts, args);

            if(line.hasOption("kafka")) {
                String hostOption = line.getOptionValue("host");
                String topicOption = line.getOptionValue("topic");
                String portOption = line.getOptionValue("port");
                String intervalOption = line.getOptionValue("interval");

                String host = hostOption != null ? hostOption : "localhost";
                String topic = topicOption != null ? topicOption : "test";
                int port = portOption != null ? Integer.parseInt(portOption) : 9092;
                int interval = intervalOption != null ? Integer.parseInt(intervalOption) : 10;

                useKafka(host, port, topic, interval);
            }
            else if(line.hasOption("files")) {
                System.out.println("writing to file");
            }

        }catch(ParseException e ) {
            System.out.println("Parsing cmd args failed: " + e.getMessage());
            System.exit(-1);
        }

    }

    public static void useKafka(String host, int port, String topic, int interval) {
        System.out.println(String.format("using kafka with settings: %s:%d, topic: %s, interval %d", host, port, topic, interval));
    }

    public static void useFiles(String[] args, Parser parser) {
        for(int i = 0; i < args.length; i++) {
            String filePath = args[i];

            String content = FileReader.readFile(filePath);
            List<AisMessage> decoded = Decoder.decode(content);

            String path = Paths.get(filePath).getFileName().toString();
            parser.writeToCsvFile(path, decoded);
        }

    }

    public static Options createOptions() {
        Options options = new Options();

        // The bool in this function indicates this option should have a value after it.
        options.addOption(new Option("k", "kafka", false, "Read from Kafka cluster"));
        options.addOption(new Option("f", "files", true, "Write output to files"));

        options.addOption(new Option("h", "host", true, "Kafka hostname"));
        options.addOption(new Option("p", "port", true, "Kafka hostname port"));
        options.addOption(new Option("t", "topic", true, "Kafka topic name"));

        options.addOption(new Option("i", "interval", true, "Set polling interval"));

        return options;
    }

}

