package com.zhmcode.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Properties;

/**
 * Created by zhmcode on 2018/12/10 0010.
 */
public class KafkaProducerDemo extends Thread{

    private final static String BROKER_LIST = "192.168.126.31:9092";
    private final static String TPOPIC = "test10";
    private Producer<Integer, String> kafkaProducer;

    public   KafkaProducerDemo() {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");
        kafkaProducer = new Producer<Integer,String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        for (int i = 0; i < 20; i++) {
            String msg = "zhmkafka"+i;
            kafkaProducer.send(new KeyedMessage<Integer, String>(TPOPIC,msg));
            System.out.println(msg);
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
