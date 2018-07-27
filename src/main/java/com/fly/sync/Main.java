package com.fly.sync;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fly.sync.setting.Setting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] argv) {

        try {

            Setting.readSettings();

        } catch (Exception e)
        {
            //logger.error(e.getMessage(), e);
            return;
        }

        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("106.75.147.144",11111), "example", "", "");
        int batchSize = 10;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();

            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    System.out.println("empty, and sleep");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    //System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);

                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

        } finally {
            System.out.println("exit manual.");
            connector.disconnect();
        }

    }

    private static Map<String, Long> idList = new HashMap<String, Long>();

    private static void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;

            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            if (!entry.getHeader().getTableName().equals("chats"))
                continue;

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                String id = getID(rowData.getAfterColumnsList());
                if (id == null || id.length() == 0 || Long.parseLong(id) < 87382309) continue;
                String key = entry.getHeader().getTableName() + "-" + id;
                switch (eventType)
                {
                    case DELETE:
                        idList.remove(key);
                        break;
                    case INSERT:
                        idList.put(key, Long.parseLong(id));
                        break;
                    default:
                        if (!idList.containsKey(key))
                            logger.info(key);
                        break;
                }
            }

            /*

            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("-------&gt; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------&gt; after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
            //*/
        }
    }

    private static String getID(List<CanalEntry.Column> columns)
    {
        for (CanalEntry.Column column : columns) {
            if (column.getName().equals("id"))
                return column.getValue();
            //System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
        return null;
    }

    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}
