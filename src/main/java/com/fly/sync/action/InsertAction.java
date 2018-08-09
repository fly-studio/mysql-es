package com.fly.sync.action;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractRecord;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.mysql.model.Record;

public class InsertAction implements AbstractAction, AbstractRecord {

    private Record record;

    public InsertAction(Record record) {
        this.record = record;
    }

    public static InsertAction create(Record record)
    {
        return new InsertAction(record);
    }

    @Override
    public void execute(DbFactory dbFactory) {


    }

    public Record getRecord() {
        return record;
    }
}
