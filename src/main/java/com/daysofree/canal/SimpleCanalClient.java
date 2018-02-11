package com.daysofree.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.daysofree.canal.model.User;

import java.net.InetSocketAddress;
import java.util.List;


/**
 * 基于canal.deployer-1.0.23.tar.gz
 */
public class SimpleCanalClient {


    public static void main(String args[]) {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("172.16.13.26",
                11111), "example", "", "");
        int batchSize = 1000;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<Entry> entries) {
        for (Entry entry : entries) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            String tableName = entry.getHeader().getTableName();
            System.out.println("表名:" + tableName);

            if ("t_user".equalsIgnoreCase(tableName)) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();
                System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                        entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                        entry.getHeader().getSchemaName(), tableName,
                        eventType));

                for (RowData rowData : rowChage.getRowDatasList()) {
                    List<Column> columns = null;
                    if (eventType == EventType.DELETE) {
                        columns = rowData.getBeforeColumnsList();
                    } else if (eventType == EventType.INSERT) {
                        columns = rowData.getAfterColumnsList();
                    } else if (eventType == EventType.UPDATE) {
                        columns = rowData.getAfterColumnsList();
                    } else {

                    }
                    User user = columns2JavaModel(columns, User.class);
                    System.err.println(JSON.toJSONString(user, true));
                }
            }

        }
    }

    private static <T> T columns2JavaModel(List<Column> columns, Class<T> clazz) {
        JSONObject obj = new JSONObject();
        for (Column column : columns) {
            obj.put(column.getName(), column.getValue());
        }
        return JSON.parseObject(obj.toJSONString(), clazz);
    }

}
