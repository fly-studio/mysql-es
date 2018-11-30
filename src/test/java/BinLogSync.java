/*
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.fly.sync.canal.Client;
import org.fly.sync.canal.Server;
import org.fly.sync.setting.BinLog;
import org.fly.sync.setting.Setting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinLogSync {

    public final static Logger logger = LoggerFactory.getLogger(BinLogSync.class);

    public static void main(String[] argv) {
        try {
            Setting.readSettings();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return;
        }

        BinLog.Position position = Setting.binLog.get(Setting.river.databases.get(0).schemaName);

        System.out.println(position);

        Server server = new Server(Setting.river, Setting.river.databases.get(0), position != null ? position : BinLog.Position.createVirtual());
        server.start();
        System.out.println("Start Server");
        new BinLogSync()
                .run(server);
    }

    public void run(Server server)
    {
        // 创建链接
        Client connector = new Client(server);
        int batchSize = 10;
        try {
            connector.subscribe();
            connector.rollback();

            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    System.out.println("empty, and sleep");
                    try {
                        Thread.sleep(1000);
                        continue;
                    } catch (InterruptedException e) {
                    }
                } else {
                    //System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);

                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据

                System.out.println("next i");
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally {
            System.out.println("exit manual.");
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


            System.out.println(String.format("================&gt; executor[%s:%s] , name[%s,%s] , eventType : %s",
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

        }
    }


    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}
*/