package com.zhmcode.kafka;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhmcode on 2018/12/10 0010.
 */
public class KafkaConsumerDemo extends Thread {

    private final static String ZOOKEEPER_LIST = "192.168.126.31:2181,192.168.126.32:2181,192.168.126.33:2181";
    private final static String GROUP_ID = "test_group10";
    private final static String TPOPIC = "test10";
    private ConsumerConnector consumerConnector;

    public  KafkaConsumerDemo() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", ZOOKEEPER_LIST);
        properties.put("group.id", GROUP_ID);
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
}

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TPOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TPOPIC).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("Receive->[" + new String(it.next().message()) + "]");
            try {
                Thread.sleep(2000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
