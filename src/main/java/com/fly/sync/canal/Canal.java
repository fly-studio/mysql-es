package com.fly.sync.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.sync.action.*;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractLifeCycle;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.exception.CanalFatalException;
import com.fly.sync.executor.Executor;
import com.fly.sync.executor.Statistic;
import com.fly.sync.mysql.MySql;
import com.fly.sync.mysql.model.Record;
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
import java.util.stream.Collectors;

public class Canal extends AbstractLifeCycle implements DbFactory {

    private Config config;
    private River river;
    private DbFactory dbFactory;
    private BinLog.Position position;
    private Server server;
    private Client client;

    public final static Logger logger = LoggerFactory.getLogger(Canal.class);

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
    public void start() {
        super.start();

        server.start();
        client.start();

    }

    @Override
    public void stop()
    {
        super.stop();

        client.unsubscribe();
        server.stop();
        client.stop();

        logger.info("Canal server stop.");
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

    @Override
    public ObjectMapper getJsonMapper() {
        return dbFactory.getJsonMapper();
    }

    public Observable<AbstractAction> run(Scheduler scheduler)
    {
        while(!server.getCanalInstance().getMetaManager().isStart())
        {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                return Observable.empty();
            }
        }

        logger.info("Canal server started.");

        client.subscribe();
        client.rollback();

        return Observable.create(new DataEmitter())
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                ;
    }

    private class DataEmitter implements ObservableOnSubscribe<AbstractAction> {

        ObservableEmitter<AbstractAction> observableEmitter;

        public DataEmitter() {

        }

        private List<AbstractAction> parseEntries(List<CanalEntry.Entry> entries) throws SQLException
        {
            List<AbstractAction> actionList = new ArrayList<>();
            List<AbstractAction> relateActionList = new ArrayList<>();

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

                boolean sync = getRiverDatabase().isSync(tableName);
                List<River.Associate> associates = getRiverDatabase().getAssociates(tableName);


                CanalEntry.EventType eventType = rowChange.getEventType();

                switch (eventType)
                {
                    case DELETE:

                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList())
                        {
                            Record record = getMySql().getLocalQuery().mixRecord(getRiverDatabase().schemaName, tableName, getBeforeColumnsList(rowData));
                            record.setDeleted();

                            if (sync)
                                actionList.add(DeleteAction.create(record));

                            for (River.Associate associate: associates
                                 )
                                relateActionList.add(DeleteRelateAction.create(record, associate));
                        }

                        break;
                    case INSERT:

                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList())
                        {
                            Record record = getMySql().getLocalQuery().mixRecord(getRiverDatabase().schemaName, tableName, getAfterColumnsList(rowData));
                            record.setInserted();

                            if (sync)
                                actionList.add(InsertAction.create(record));

                            for (River.Associate associate: associates
                            )
                                relateActionList.add(InsertRelateAction.create(record, associate));
                        }

                        break;
                    case UPDATE:

                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList())
                        {
                            Record record = getMySql().getLocalQuery().mixRecord(getRiverDatabase().schemaName, tableName, getAfterColumnsList(rowData));
                            record.setUpdated(getModifiedColumns(rowData.getAfterColumnsList()));

                            if (sync)
                                actionList.add(UpdateAction.create(record));

                            for (River.Associate associate: associates
                            )
                                relateActionList.add(UpdateRelateAction.create(record, associate));
                        }

                        break;
                    case ALTER:

                        //TODO
                        break;
                }
            }

            // append
            actionList.addAll(relateActionList);

            return actionList;
        }

        private List<String> getModifiedColumns(List<CanalEntry.Column> columns)
        {
            return columns.stream().filter(CanalEntry.Column::getUpdated).map(CanalEntry.Column::getName).collect(Collectors.toList());
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

            while (Executor.isRunning() && isStart()) {

                if (getStatistic().getDumpCount().get() + getStatistic().getCanalCount().get() - getStatistic().getRecordCount().get() > config.bulkSize * 5)
                {
                    //logger.info("Canal {} and subscribe {}, sleep 0.1s", total, getRecordCount.get());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e)
                    {
                        break;
                    }
                    continue;
                }

                try {

                    Message message = client.getWithoutAck(config.bulkSize, (long) config.flushBulkTime, TimeUnit.MILLISECONDS);

                    long batchId = message.getId();
                    int size = message.getEntries().size();

                    if (batchId == -1 || size == 0) {
                        Thread.sleep(config.flushBulkTime);
                        continue;
                    }

                    List<AbstractAction> actionList = parseEntries(message.getEntries());
                    if (!actionList.isEmpty()) {
                        getStatistic().getCanalCount().addAndGet(actionList.size());

                        for (AbstractAction action : actionList
                        ) {
                            observableEmitter.onNext(action);
                        }
                    }

                    client.ack(batchId); // 提交确认

                    BinLog.Position position = server.getBinLogPosition();
                    if (position != null)
                        observableEmitter.onNext(new ChangePositionAction(position));

                } catch (InterruptedException e)
                {
                    break;
                } catch (Exception e)
                {
                    if (e.getCause() instanceof  InterruptedException)
                        break;

                    if (client.isStart())
                        client.rollback();

                    observableEmitter.onError(new CanalFatalException(e));
                    break;
                }
            }

            observableEmitter.onComplete();
        }
    }
}
