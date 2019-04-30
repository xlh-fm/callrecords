package com.xu.consumer;

import com.xu.utils.ResourcesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Properties;

public class HBaseConsumer {
    public static void main(String[] args) throws IOException, ParseException {

        //get kafka configuration information
        Properties properties = ResourcesUtil.getProperties();

        //create a Kafka consumer and subscribe to the topic
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(properties.getProperty("kafka.topic")));

        HBaseDAO dao = new HBaseDAO();

        //loop get data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                    dao.putData(record.value());
                }
            }
        } finally {
            dao.close();
        }
    }
}