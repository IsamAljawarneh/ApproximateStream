

package com.unibo.kafkaProducerPack;

import java.util.stream.Stream;

/**
 * @Description:
 * @author: Isam Al Jawarneh
 * @date: 12/03/2021
 * run: mvn clean package
 *
 * java -jar saosKafkaProducer-1.0-SNAPSHOT.jar
 * java -jar saosKafkaProducer-1.0-SNAPSHOT.jar shenzhen spatial1 localhost:9092 /home/isam/Desktop/spatial/data/china/points/guang.csv 1
 * java -jar saosKafkaProducer-1.0-SNAPSHOT.jar shenzhen spatial1 localhost:9092 /home/isam/Desktop/spatial/data/china/datacsv/points2.csv 1
 * java -jar saosKafkaProducer-1.0-SNAPSHOT.jar nyc spatial localhost:9092 /home/isam/Desktop/spatial/data/NYC_trips1/nyc.csv 1
 * java -jar kafka-producer-consumer.jar shenzhen spatial1 wn0-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092,wn1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092 /home/isam/guang.csv 1
 */
public class SendMessageApplication {

    public static void main(String[] args) throws Exception {

        if(args.length < 5) {
            usage();
        }

        // Get the brokers
        String topicName = args[1];
        String brokers = args[2];
        String path = args[3];
        String data = args[0];
        int time =Integer.parseInt(args[4]);

        switch(args[0].toLowerCase()) {

            case "nyc":
                Stream.generate(new NYCsvReader(path))
                        .sequential()
                .forEachOrdered(new KafkaProducer(topicName, brokers,time));
                break;
            case "shenzhen":
                Stream.generate(new shenzhenCSVreader(path))
                        .sequential()
                        .forEachOrdered(new KafkaProducerShenzhen(topicName, brokers,time));

                break;
        }
        //String path = "/home/isam/Desktop/spatial/data/NYC_trips1/nyc.csv";
        //String path = "/home/isam/Desktop/spatial/data/china/points/guang.csv";

        // kafka topic
        //String kafka_topic = "spatial";
        //String kafka_topic = "spatial1";

        // kafka borker
        //String kafka_broker = "localhost:9092";

        //String kafka_broker = "wn0-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092,wn1-skafka.j5rjzygn4qce1gsf4rcdijhweg.fx.internal.cloudapp.net:9092";



    }

    // Display usage
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("kafka-example.jar <producer|consumer|describe|create|delete> <topicName> brokerhosts [groupid]");
        System.exit(1);
    }
}
