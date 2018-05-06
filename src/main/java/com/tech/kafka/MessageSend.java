package com.tech.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

/**
 * @author xxx_xx
 * @date 2018/4/13
 */
public class MessageSend extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private BlockingQueue<String> queue1;
    private Logger logger = LoggerFactory.getLogger(MessageSend.class);

    public MessageSend(String topic, Boolean isAsync, BlockingQueue<String> queue) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "centos-1:9092,centos-2:9092,centos-3:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        queue1 = queue;
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String message = "";
            try {
                message = queue1.take();
                long startTime = System.currentTimeMillis();
                if (isAsync && !message.equals("")) {
                    producer.send(new ProducerRecord<>(topic,
                            message), new DemoCallBack(startTime, messageNo, message));
                } else {
                    try {
                        producer.send(new ProducerRecord<>(topic,
                                messageNo,
                                message)).get();
                        logger.info("Sent message: (" + messageNo + ", " + message + ")");
                    } catch (InterruptedException e) {
                        logger.error("同步发送到kafka失败", e);
                    } catch (ExecutionException e) {
                        logger.error("同步发送到kafka失败", e);
                    }
                }
                ++messageNo;
            } catch (InterruptedException e) {
                logger.error("get data error", e);
                e.printStackTrace();
            }
        }
    }
}
