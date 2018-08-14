package com.fly.sync.action;

import com.fly.sync.contract.AbstractRecordAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.mysql.model.Record;
import com.fly.sync.setting.River;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateAction implements AbstractRecordAction {

    public final static Logger logger = LoggerFactory.getLogger(UpdateAction.class);

    private Record record;

    public UpdateAction(Record record) {
        this.record = record;
    }

    public static UpdateAction create(Record record) {
        return new UpdateAction(record);
    }

    @Override
    public void execute(DbFactory dbFactory) {
        dbFactory.getStatistic().getUpdateCount().incrementAndGet();
    }

    @Override
    public DocWriteRequest getRequest(DbFactory dbFactory) throws Exception {

        River.Table table = dbFactory.getRiverDatabase().getTable(record.table);

        return new IndexRequest(table.index, table.type, record.getID(table))
                .source(record.toJson(dbFactory.getJsonMapper()), XContentType.JSON);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public String getGroup() {
        return UpdateAction.class.getName();
    }
}
