package org.fly.sync.mysql.relation;

import org.fly.sync.action.InsertAction;
import org.fly.sync.action.UpdateAction;
import org.fly.sync.contract.*;
import org.fly.sync.mysql.model.Record;
import org.fly.sync.setting.River;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Relation implements AbstractRelation {

    private DbFactory dbFactory;
    private Map<String, RelationRecords> tableRecords = new HashMap<>();

    public Relation(DbFactory dbFactory, List<AbstractAction> actionList) {

        this.dbFactory = dbFactory;

        filterToTableActions(actionList);
    }

    private River.Database getRiverDatabase()
    {
        return dbFactory.getRiverDatabase();
    }

    private void filterToTableActions(List<AbstractAction> actionList)
    {
        tableRecords.clear();

        for (AbstractAction action :actionList
        ) {
            if (!(action instanceof AbstractRecordAction))
                continue;

            Record record = ((AbstractRecordAction) action).getRecord();

            if (action instanceof InsertAction || action instanceof UpdateAction)
            {
                if (getRiverDatabase().hasWith(record.table))
                    putToTableAction(record.table, record);
            } else if (action instanceof AbstractRelateAction)
                putToTableAction(record.table, ((AbstractRelateAction) action).getAssociate(), record);
        }
    }

    private void putToTableAction(String tableName, River.Associate associate, Record record)
    {
        RelationRecords records;

        String key = River.makeRelationKey(tableName, associate == null ? "" : associate.relationKey);

        if (tableRecords.containsKey(key))
        {
            records = tableRecords.get(key);
        } else {
            records = new RelationRecords(dbFactory, tableName, associate);
            tableRecords.put(key, records);
        }

        records.addRecord(record);
    }

    private void putToTableAction(String tableName, Record record)
    {
        putToTableAction(tableName, null, record);
    }

    public void load()
    {
        for (Map.Entry<String, RelationRecords> entry: tableRecords.entrySet()
        ) {
            entry.getValue().relate();
        }
    }

}
