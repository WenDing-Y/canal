package com.tech.canal;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class CanalExportClient extends Thread {
    private BlockingQueue<String> queue;
    private Logger logger = LoggerFactory.getLogger(CanalExportClient.class);

    public CanalExportClient(BlockingQueue<String> queue) {
        this.queue = queue;
    }

    public void init() {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                11111), "example", "canal", "112233@Lsy");
        int batchSize = 1000;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            while (true) {
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                        logger.info("empty");
                    } catch (InterruptedException e) {
                    }
                } else {
                    processData(message.getEntries());
                }
                connector.ack(batchId);
            }
        } finally {
            connector.disconnect();
        }
    }

    private void processData(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if ((entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) || (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND)) {
                continue;
            }
            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            EventType eventType = rowChage.getEventType();
            String schemaName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();
            String eventName = String.format("%s", new Object[]{eventType});
            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    buildJsonMessage(rowData.getBeforeColumnsList(), schemaName, tableName, eventName);
                } else {
                    buildJsonMessage(rowData.getAfterColumnsList(), schemaName, tableName, eventName);
                }
            }

        }
    }

    private void buildJsonMessage(List<CanalEntry.Column> columns, String schemaName, String tableName, String eventName) {
        JSONObject object = new JSONObject();
        object.put("schemaName", schemaName);
        object.put("tableName", tableName);
        object.put("eventName", eventName);
        JSONArray array = new JSONArray();
        JSONObject object1 = null;
        for (CanalEntry.Column column : columns) {
            object1 = new JSONObject();
            object1.put(column.getName(), column.getValue());
            array.add(object1);
        }
        object.put("column",array.toJSONString());
        try {
            this.logger.info(object.toString());
            this.queue.offer(object.toString(), 2L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            this.logger.error("提交数据到缓冲区失败", e);
        }
    }

    public void run() {
        init();
    }
}