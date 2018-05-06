package com.tech;



import com.tech.canal.CanalExportClient;
import com.tech.kafka.MessageSend;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author xxx_xx
 * @date 2018/4/13
 */
public class Launcher {

    public static void main(String[] args) {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        ExecutorService service = Executors.newFixedThreadPool(2);

        service.submit(new CanalExportClient(queue));
        service.submit(new MessageSend("real-table-data", false, queue));

        service.shutdown();

    }
}
