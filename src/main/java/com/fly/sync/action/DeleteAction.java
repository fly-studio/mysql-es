package com.fly.sync.action;

import com.fly.sync.contract.AbstractRecordAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.mysql.model.Record;
import com.fly.sync.setting.River;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteAction implements AbstractRecordAction {
    public final static Logger logger = LoggerFactory.getLogger(DeleteAction.class);

    private Record record;

    public DeleteAction(Record record) {
        this.record = record;
    }

    public static DeleteAction create(Record record) {
        return new DeleteAction(record);
    }

    @Override
    public void execute(DbFactory dbFactory) {
        dbFactory.getStatistic().getDeleteCount().incrementAndGet();
    }

    @Override
    public DocWriteRequest getRequest(DbFactory dbFactory) throws Exception {
        River.Table table = dbFactory.getRiverDatabase().getTable(record.table);

        return new DeleteRequest(table.index, table.type, record.getID(table));
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public String getGroup() {
        return DeleteAction.class.getName();
    }


}
