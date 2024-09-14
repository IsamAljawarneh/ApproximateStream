
package com.unibo.kafkaProducerPack;

import com.unibo.beans.Nyc;
import com.unibo.sede.JsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.function.Consumer;

/**
 * @Description:
 * @author: Isam Al Jawarneh
 * @date: 2021/04/2
 */
public class KafkaProducer implements Consumer<Nyc> {

    private final String topic;
    private final org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> producer;
    private final int sleepTime;

    private final JsonSerializer<Nyc> serializer;

    public KafkaProducer(String kafkaTopic, String kafkaBrokers, int time) {
        this.topic = kafkaTopic;
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(createKafkaProperties(kafkaBrokers));
this .sleepTime = time;
        this.serializer = new JsonSerializer<>();
    }

    @Override
    public void accept(Nyc record) {
        //
        byte[] data = serializer.toJSONBytes(record);
        byte[] key = record.getDriver_id().getBytes();
        //
        ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord<>(topic,key, data);
        //
        producer.send(kafkaRecord);
        System.out.println("key" + kafkaRecord.key().toString());

        // sleeping for sometime
        try {
            Thread.sleep(sleepTime);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    /**
     * kafka
     * @param brokers  brokers used for connection.
     * @return kafka producer config.
     */
    private static Properties createKafkaProperties(String brokers) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }

}
