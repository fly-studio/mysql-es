package org.fly.sync.action;

import org.fly.sync.contract.AbstractRecordAction;
import org.fly.sync.contract.AbstractWriter;
import org.fly.sync.contract.DbFactory;
import org.fly.sync.mysql.model.Record;
import org.fly.sync.setting.River;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
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

    public void execute(AbstractWriter writer) {
        writer.getDbFactory().getStatistic().getUpdateCount().incrementAndGet();
    }

    @Override
    public DocWriteRequest getRequest(DbFactory dbFactory) throws Exception {

        River.Table table = dbFactory.getRiverDatabase().getTable(record.table);

        return new UpdateRequest(table.buildIndexName(record), table.type, record.getID(table))
                .doc(record.toJson(dbFactory.getJsonMapper()), XContentType.JSON);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public String getGroup() {
        return AbstractRecordAction.class.getName();
    }
}
