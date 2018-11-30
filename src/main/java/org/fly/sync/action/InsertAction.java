package org.fly.sync.action;

import org.fly.sync.contract.AbstractRecordAction;
import org.fly.sync.contract.AbstractWriter;
import org.fly.sync.contract.DbFactory;
import org.fly.sync.mysql.model.Record;
import org.fly.sync.setting.River;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertAction implements AbstractRecordAction {

    public final static Logger logger = LoggerFactory.getLogger(InsertAction.class);
    private Record record;

    public InsertAction(Record record) {
        this.record = record;
    }

    public static InsertAction create(Record record)
    {
        return new InsertAction(record);
    }

    @Override
    public void execute(AbstractWriter writer) {
        writer.getDbFactory().getStatistic().getInsertCount().incrementAndGet();
    }

    @Override
    public DocWriteRequest getRequest(DbFactory dbFactory) throws Exception {

        River.Table table = dbFactory.getRiverDatabase().getTable(record.table);

        return new IndexRequest(table.buildIndexName(record), table.type, record.getID(table))
                .source(record.toJson(dbFactory.getJsonMapper()), XContentType.JSON);
    }

    public Record getRecord() {
        return record;
    }

    @Override
    public String getGroup() {
        return AbstractRecordAction.class.getName();
    }
}
