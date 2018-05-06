package com.tech.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xxx_xx
 * @date 2018/4/13
 */
class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;
    private Logger logger = LoggerFactory.getLogger(DemoCallBack.class);

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    //得到消息分区位置，和偏移量
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            logger.info(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            logger.error("回调函数出错", exception);
        }
    }
}
