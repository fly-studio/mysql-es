package com.fly.sync.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fly.sync.action.ChangePositionAction;
import com.fly.sync.action.DeleteAction;
import com.fly.sync.action.InsertAction;
import com.fly.sync.action.UpdateAction;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalCanalException;
import com.fly.sync.executor.Executor;
import com.fly.sync.executor.Statistic;
import com.fly.sync.mysql.Dumper;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.Config;
import com.fly.sync.setting.River;
import com.sun.istack.internal.NotNull;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Canal implements DbFactory {

    private Config config;
    private River river;
    private DbFactory dbFactory;
    private BinLog.Position position;
    private Server server;
    private Client client;

    public final static Logger logger = LoggerFactory.getLogger(Dumper.class);

    public Canal(@NotNull Config config, @NotNull River river, BinLog.Position position, DbFactory dbFactory) {
        this.config = config;
        this.river = river;
        this.position = position;
        this.dbFactory = dbFactory;

        init();
    }

    private void init()
    {
        server = new Server(river, getRiverDatabase(), position);
        client = new Client(server);
    }

    @Override
    public Es getEs() {
        return dbFactory.getEs();
    }

    @Override
    public MySql getMySql() {
        return dbFactory.getMySql();
    }

    @Override
    public River.Database getRiverDatabase() {
        return dbFactory.getRiverDatabase();
    }

    @Override
    public Statistic getStatistic() {
        return dbFactory.getStatistic();
    }

    public Observable<AbstractAction> run(Scheduler scheduler)
    {
        server.start();
        while(!server.getCanalInstance().getMetaManager().isStart())
        {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        }

        client.subscribe();
        client.rollback();

        return Observable.create(new DataEmitter())
                .observeOn(scheduler)
                .subscribeOn(scheduler);
    }

    public void stop()
    {
        if (null != server)
        {
            client.unsubscribe();
            server.stop();
            server = null;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        stop();
    }

    private class DataEmitter implements ObservableOnSubscribe<AbstractAction> {

        ObservableEmitter<AbstractAction> observableEmitter;

        public DataEmitter() {

        }

        private List<AbstractAction> parseEntries(List<CanalEntry.Entry> entries) throws SQLException
        {
            List<AbstractAction> actionList = new ArrayList<>();

            for (CanalEntry.Entry entry : entries) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    continue;
                }

                CanalEntry.RowChange rowChange = null;

                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    logger.error("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
                    continue;
                }

                String tableName = entry.getHeader().getTableName();

                CanalEntry.EventType eventType = rowChange.getEventType();

                switch (eventType)
                {
                    case DELETE:

                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList())
                            actionList.add(DeleteAction.create(getMySql().mixRecord(getRiverDatabase().schemaName, tableName, getBeforeColumnsList(rowData))));

                    break;
                    case INSERT:

                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList())
                            actionList.add(InsertAction.create(getMySql().mixRecord(getRiverDatabase().schemaName, tableName, getAfterColumnsList(rowData))));

                        break;
                    case UPDATE:

                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList())
                            actionList.add(UpdateAction.create(getMySql().mixRecord(getRiverDatabase().schemaName, tableName, getAfterColumnsList(rowData))));

                        break;
                    case ALTER:
                        //TODO
                        break;
                }
            }
            return actionList;
        }

        private Map<String, String> getBeforeColumnsList(CanalEntry.RowData rowData)
        {
            Map<String, String> items = new HashMap<>();

            for (CanalEntry.Column column: rowData.getBeforeColumnsList()
                 ) {
                items.put(column.getName(), column.getIsNull() ? null : column.getValue());
            }

            return items;
        }

        private Map<String, String> getAfterColumnsList(CanalEntry.RowData rowData)
        {
            Map<String, String> items = new HashMap<>();

            for (CanalEntry.Column column: rowData.getAfterColumnsList()
            ) {
                items.put(column.getName(), column.getIsNull() ? null : column.getValue());
            }

            return items;
        }

        @Override
        public void subscribe(ObservableEmitter<AbstractAction> observableEmitter) throws Exception {
            this.observableEmitter = observableEmitter;

            while (Executor.isRunning()) {

                if (getStatistic().getDumpCount().get() + getStatistic().getCanalCount().get() - getStatistic().getRecordCount().get() > config.bulkSize * 5)
                {
                    //logger.info("Canal {} and subscribe {}, sleep 0.1s", total, getRecordCount.get());
                    //Thread.sleep(100);
                    continue;
                }

                try {

                    Message message = client.getWithoutAck(config.bulkSize, (long) config.bulkSize, TimeUnit.MILLISECONDS);

                    long batchId = message.getId();
                    int size = message.getEntries().size();

                    if (batchId == -1 || size == 0) {
                        Thread.sleep(1000);
                        continue;
                    }

                    List<AbstractAction> actionList = parseEntries(message.getEntries());
                    if (!actionList.isEmpty())
                    {
                        getStatistic().getCanalCount().addAndGet(actionList.size());

                        for (AbstractAction action: actionList
                             ) {
                            observableEmitter.onNext(action);
                        }

                    }

                    client.ack(batchId); // 提交确认

                    BinLog.Position position = server.getBinLogPosition();
                    if (position != null)
                        observableEmitter.onNext(new ChangePositionAction(position));

                } catch (Exception e)
                {
                    client.rollback();
                    observableEmitter.onError(new FatalCanalException(e));
                    return;
                }
            }
        }
    }
}
