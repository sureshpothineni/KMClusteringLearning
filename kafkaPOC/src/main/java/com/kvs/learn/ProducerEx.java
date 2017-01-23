package com.kvs.learn;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

/**
 * This producer will send a bunch of messages to topic "kvsKafka". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class ProducerEx {
    public static void main(String[] args) throws IOException {
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try {
        	while(true){
            for (int i = 0; i < 10; i++) {
                // send lots of messages
                producer.send(new ProducerRecord<String, String>(
                        "kvsKafka1",
                        String.format("{\"type\":\"this is common message\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                System.out.println("Sent msg number " + i);

                // every so often send to a different topic
                if (i % 10 == 0) {
                    producer.send(new ProducerRecord<String, String>(
                            "test",
                            String.format("{\"type\":\"this is special message\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                   
                    producer.flush();
                    System.out.println("Sent msg number " + i);
                }
                
            }
            TimeUnit.SECONDS.sleep(15);
      	}
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }
}