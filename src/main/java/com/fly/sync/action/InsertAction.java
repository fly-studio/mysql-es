package com.fly.sync.action;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.mysql.model.Record;

import java.util.Map;

public class InsertAction extends Record implements AbstractAction {

    public InsertAction(String table, Map<String, Object> items) {
        super(table, items);
    }


    public static InsertAction create(Record record)
    {
        return new InsertAction(record.table, record.items);
    }

}
