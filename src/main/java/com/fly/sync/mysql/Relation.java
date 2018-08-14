package com.fly.sync.mysql;

import com.fly.sync.action.InsertAction;
import com.fly.sync.action.UpdateAction;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractRecordAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.mysql.model.Record;
import com.fly.sync.mysql.model.Records;
import com.fly.sync.setting.River;
import io.reactivex.annotations.Nullable;

import java.util.*;

public class Relation {

    private DbFactory dbFactory;
    private Records recordList;
    private Map<String, RelationRecords> tableRecords = new HashMap<>();

    public Relation(DbFactory dbFactory, Records actionList) {

        this.dbFactory = dbFactory;
        this.recordList = actionList;

        filterToTableActions();
    }

    public Relation(DbFactory dbFactory, List<AbstractAction> actionList) {

        this.dbFactory = dbFactory;

        recordList = new Records();
        for (AbstractAction o: actionList
        ) {
            if (o instanceof InsertAction || o instanceof UpdateAction)
                recordList.add(((AbstractRecordAction) o).getRecord());
        }

        filterToTableActions();
    }

    private River.Database getRiverDatabase()
    {
        return dbFactory.getRiverDatabase();
    }

    private void filterToTableActions()
    {
        tableRecords.clear();

        for (Record record :recordList
        ) {
            if (getRiverDatabase().hasWith(record.table))
                putToTableAction(record.table, record);
        }
    }

    private void putToTableAction(String tableName, Record action)
    {
        RelationRecords records;
        if (tableRecords.containsKey(tableName))
        {
            records = tableRecords.get(tableName);
        } else {
            records = new RelationRecords(dbFactory, tableName);
            tableRecords.put(tableName, records);
        }

        records.addAction(action);
    }

    public void load()
    {
        // for each tables
        for (Map.Entry<String, RelationRecords> entry: tableRecords.entrySet()
        ) {
            List<String> withNames = entry.getValue().table.getFullWiths();

            for (String withName: withNames
                 ) {
                River.Associate associate = getRiverDatabase().findAssociate(withName);
                if (associate != null)
                    entry.getValue().with(associate);
            }
        }
    }

    public class RelationRecords {
        River.Table table;
        Records records = new Records();
        List<String> loadedRelationKeys = new ArrayList<>();
        private DbFactory dbFactory;

        RelationRecords(DbFactory dbFactory, String tableName) {
            this.dbFactory = dbFactory;
            this.table = getRiverTable(tableName);
        }

        void addAction(Record action)
        {
            records.add(action);
        }

        River.Table getRiverTable(String tableName)
        {
            return dbFactory.getRiverDatabase().getTable(tableName);
        }

        void with(River.Associate associate)
        {
            if (loadedRelationKeys.contains(associate.relationKey))
                return;

            List<String> relationKeys = associate.getRelationKeyList();
            List<String> localValues;
            Records localRecords;
            River.Relation relation;
            String localPrefixKey, relationPrefixKey;

            for(int i = 0; i < associate.nestedRelations.size(); ++i)
            {
                localPrefixKey = String.join(".", relationKeys.subList(0, i));
                relationPrefixKey = String.join(".", relationKeys.subList(0, i + 1));

                relation = associate.nestedRelations.get(i);

                localValues = getLocalValues(records, relation, localPrefixKey);
                localRecords = getRelationRecords(localValues, relation);

                fillRecords(records, relation, localRecords, localPrefixKey, relationPrefixKey);

                loadedRelationKeys.add(relationPrefixKey);
            }

        }

        private void fillRecords(List<Record> originalRecords, River.Relation relation, Records relationRecords, String localPrefixKey, String relationPrefixKey)
        {
            Record nullRecord = Record.createNull(relation.tableName, relation.getColumns());
            String localColumn = localPrefixKey.isEmpty() ? relation.local : localPrefixKey + "." + relation.local;

            for (Record record: originalRecords
                 ) {
                Object val = record.get(localColumn);

                if (val == null) {
                    record.with(relationPrefixKey, nullRecord);
                } else {

                    Record relationRecord = relationRecords.find(relation.foreign, val);

                    record.with(relationPrefixKey, relationRecord == null ? nullRecord : relationRecord);
                }
            }
        }

        @Nullable
        private Records getRelationRecords(List<String> localValues, River.Relation relation)
        {
            if (localValues == null || localValues.isEmpty())
                return null;

            return dbFactory.getMySql().getLocalQuery().queryIn(relation, relation.foreign, localValues);
        }

        @Nullable
        private List<String> getLocalValues(List<Record> originalRecords, River.Relation relation, String localPrefixKey)
        {
            Set<String> localValues = new HashSet<>();

            String localColumn = localPrefixKey.isEmpty() ? relation.local : localPrefixKey + "." + relation.local;

            for (Record record: originalRecords
                 ) {
                Object object = record.get(localColumn);
                if (object == null)
                    continue;

                localValues.add(object.toString());
            }

            return localValues.isEmpty() ? null : Arrays.asList(localValues.toArray(new String[0]));
        }

    }

}
