package com.zhmcode.kafka;

/**
 * Created by zhmcode on 2018/12/10 0010.
 */
public class TestProducerAndConsumer {

    public static void main(String[] args) {
        new KafkaProducerDemo().start();
        new KafkaConsumerDemo().start();
    }
}
