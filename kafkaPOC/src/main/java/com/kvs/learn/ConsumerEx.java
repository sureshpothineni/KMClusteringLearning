package com.kvs.learn;



import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

/**
 * This program reads messages from two topics. Messages on "fast-messages" are analyzed
 * to estimate latency (assuming clock synchronization between producer and consumer).
 * <p/>
 * Whenever a message is received on "slow-messages", the stats are dumped.
 */
public class ConsumerEx {
	public static void main(String[] args) throws IOException, InterruptedException {
		// set up house-keeping
		ObjectMapper mapper = new ObjectMapper();

		// and the consumer
		KafkaConsumer<String, String> consumer;
		InputStream props = Resources.getResource("consumer.props").openStream();
		Properties properties = new Properties();
		properties.load(props);
		if (properties.getProperty("group.id") == null) {
			properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
		}
		consumer = new KafkaConsumer<>(properties);
		props.close();
		consumer.subscribe(Arrays.asList("kvsKafka1", "test"));
		int timeouts = 0;
		//noinspection InfiniteLoopStatement
		while (timeouts <= 100) {
			// read records with a short timeout. If we time out, we don't really care.
			ConsumerRecords<String, String> records = consumer.poll(200);
			if (records.count() == 0) {
				timeouts++;
				System.out.println("waiting for msgs");
				TimeUnit.SECONDS.sleep(1);
				if(timeouts == 100){
					consumer.close();
				}
			} else {
				System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
				timeouts = 0;
			}
			for (ConsumerRecord<String, String> record : records) {
				String topic = record.topic();
				System.out.printf("topic: %s, Received message:%s\n",topic,record.value());
				
			}
		}

	}
}