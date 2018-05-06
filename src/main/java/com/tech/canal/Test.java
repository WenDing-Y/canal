package com.tech.canal;

import java.net.InetSocketAddress;
import java.util.List;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;


public class Test {


    public static void main(String args[]) {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                11111), "example", "canal", "112233@Lsy");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmptyCount = 120;
            while (true) {
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    try {
                        Thread.sleep(1000);
                        System.out.println("empty");
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    processData(message.getEntries());
                }
                connector.ack(batchId);
            }
        } finally {
            connector.disconnect();
        }
    }

    private static void processData(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            String schemaName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();
            String eventName = String.format("%s", eventType);
            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    buildJsonMessage(rowData.getBeforeColumnsList(), schemaName, tableName, eventName);
                } else {
                    buildJsonMessage(rowData.getAfterColumnsList(), schemaName, tableName, eventName);
                }
            }
        }
    }


    private static String buildJsonMessage(List<Column> columns, String schemaName, String tableName, String eventName) {
        JSONObject object = new JSONObject();
        object.put("schemaName", schemaName);
        object.put("tableName", tableName);
        object.put("eventName", eventName);
        for (Column column : columns) {
            object.put(column.getName(), column.getValue());
        }
        System.out.println(object.toJSONString());
        return object.toJSONString();
    }
}
