package com.zhmcode.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * Created by zhmcode on 2018/12/10 0010.
 */
public class KafkaProducerDemo {

    private final static String BROKER_LIST = "192.168.126.31:9092";
    private final static String TPOPIC = "test10";

    public static void  producer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer,String>(properties);
        for (int i = 0; i < 10; i++) {
            try {
                kafkaProducer.send(new ProducerRecord<Integer, String>(TPOPIC,"zhmkafka"+i));
                Thread.sleep(2000);
                System.out.println("Receive->["+"zhmkafka"+i+"]");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        kafkaProducer.close();
    }

}
