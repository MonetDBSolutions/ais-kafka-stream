package com.monetdb.ais;

import java.util.*;
import java.io.FileWriter;
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
                double interval = intervalOption != null ? Double.parseDouble(intervalOption) : 10;

                if(line.hasOption("output-path")) {
                    NewFileWriter fileWriter = new NewFileWriter(line.getOptionValue("output-path"));
                    useKafka(host, port, topic, interval, fileWriter);
                }
            }
            else if(line.hasOption("files")) {
                System.out.println("reading from file");
            }

        }catch(ParseException e ) {
            System.out.println("Parsing cmd args failed: " + e.getMessage());
            System.exit(-1);
        }

    }

    public static<T extends IOutputWriter> void useKafka(String host, int port, String topic, double interval, T writer) {
        KafkaToMonetDB kafka = new KafkaToMonetDB(host, port, topic);
        Parser parser = new Parser();
        
        while(true) {
            try {
                Thread.sleep((long)(interval * 1000));

                List<AisMessage> msgs = new ArrayList<AisMessage>();

                List<String> kafkaValues = kafka.getMessagesFromKafka();
                if(kafkaValues.size() <= 0) {
                    continue;
                }

                for(String value : kafkaValues) {
                    Tuple msgTuple = Decoder.decodeSingleMessage(value);

                    if(msgTuple.isMessageSet()) {
                        msgs.add(msgTuple.message);
                    }
                }

                List<List<String>> parsedMessages = parser.parse(msgs);
                writer.Write(parsedMessages);

            }catch(InterruptedException e ) {
                System.out.println("Caught exception: " + e.getLocalizedMessage());
                System.exit(0);
            }
        }
    }

    public static void useFiles(String[] args, Parser parser) {
        for(int i = 0; i < args.length; i++) {
            String filePath = args[i];

            String content = FileReader.readFile(filePath);
            List<AisMessage> decoded = Decoder.decode(content);

            String path = Paths.get(filePath).getFileName().toString();
            // parser.writeToCsvFile(path, decoded);
        }

    }

    public static Options createOptions() {
        Options options = new Options();

        // The bool in this function indicates this option should have a value after it.
        options.addOption(new Option("k", "kafka", false, "Read from Kafka cluster"));
        options.addOption(new Option("f", "files", true, "Read from files"));
        options.addOption(new Option("o", "output-path", true, "The path the output csv's need to be written to"));

        options.addOption(new Option("h", "host", true, "Kafka hostname"));
        options.addOption(new Option("p", "port", true, "Kafka hostname port"));
        options.addOption(new Option("t", "topic", true, "Kafka topic name"));

        options.addOption(new Option("i", "interval", true, "Set polling interval"));

        return options;
    }

}

