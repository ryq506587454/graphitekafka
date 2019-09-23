package com.ryq.Kafka;

import com.ryq.Graphite.GraphiteSender;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final List<String> topicNames;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final GraphiteSender graphiteSender;

    private KafkaConsumer<String,String> consumer;

    public Consumer(List<String> topicNames){
        this.topicNames = topicNames;

        graphiteSender = new GraphiteSender();
    }

    public void close() {
        logger.info("Waking up consumer...");
        consumer.wakeup();

        try {
            logger.info("Waiting for consumer to shutdown...");
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Exception thrown waiting for shutdown", e);
        }

    }

    public void run() {
        Properties consumerProperties = getConsumerProperties();
        consumer = new KafkaConsumer<>(consumerProperties);
        //添加订阅
        consumer.subscribe(topicNames);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                if (!records.isEmpty()) {
                    //graphiteSender.send(records);
                    records.forEach(record ->
                       System.out.println(record.value())
                    );
                }
            }
        }
        catch(WakeupException ex) {
            logger.info("Consumer has received instruction to wake up");
        }
        finally {
            logger.info("Consumer closing...");
            consumer.close();
            shutdownLatch.countDown();
            logger.info("Consumer has closed successfully");
        }
    }

    private Properties getConsumerProperties() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return configProperties;
    }


}
