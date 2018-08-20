package com.fly.sync.mysql.relation;

import com.fly.sync.contract.DbFactory;
import com.fly.sync.exception.RelationException;
import com.fly.sync.mysql.model.Record;
import com.fly.sync.mysql.model.Records;
import com.fly.sync.setting.River;
import com.sun.istack.NotNull;
import io.reactivex.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class RelationRecords {

    private String tableName;
    private River.Associate associate = null;
    private Records records = new Records();
    private Map<String, Records> loadedRelations = new HashMap<>();
    private DbFactory dbFactory;

    public RelationRecords(DbFactory dbFactory, String tableName) {
        this.dbFactory = dbFactory;
        this.tableName = tableName;
    }

    public RelationRecords(DbFactory dbFactory, String tableName, River.Associate associate) {
        this.dbFactory = dbFactory;
        this.tableName = tableName;
        this.associate = associate;
    }

    void addRecord(Record record)
    {
        records.add(record);
    }

    private River.Table getRiverTable(String tableName)
    {
        return dbFactory.getRiverDatabase().getTable(tableName);
    }

    public void relate()
    {
        if (associate == null)
            relateByWiths();
        else
            relateByAssociate(associate);
    }

    public void relateBySubAssociate(River.Associate associate, int relationOffset)
    {
        if (records.isEmpty() || loadedRelations.containsKey(associate.relationKey) || relationOffset < 0 || relationOffset >= associate.nestedRelations.size())
            return;

        Records lastRecords = records;

        for(int i = relationOffset; i < associate.nestedRelations.size(); ++i)
            lastRecords = generateOffsetRecords(associate, lastRecords, i);
    }

    public void relateByAssociate(River.Associate associate)
    {
        if (records.isEmpty())
            return;

        relateBySubAssociate(associate, associate.findOffset(tableName));
    }

    public void relateByWiths()
    {
        if (records.isEmpty())
            return;

        River.Table table = getRiverTable(tableName);
        if (table == null)
            throw new RelationException("This Table ["+dbFactory.getRiverDatabase().schemaName+"."+tableName+"] is NOT a Syncable table.");

        for (String withName: table.getFullWiths()
        ) {
            River.Associate associate = dbFactory.getRiverDatabase().findAssociate(withName);

            relateBySubAssociate(associate, 0);
        }

    }

    private Records generateOffsetRecords(River.Associate associate, @NotNull Records originalRecords, int relationOffset)
    {
        List<String> relationKeys = associate.getRelationKeyList();
        String key = River.makeRelationKey(associate.calledTable.tableName, relationKeys.subList(0, relationOffset + 1));

        Records records = loadedRelations.get(key);

        if (records == null)
        {
            River.Relation relation = associate.nestedRelations.get(relationOffset);
            Records needRelateRecords = originalRecords.stream()
                    .filter(record -> record.isModified(relation.local))
                    //.collect(Records::new, Records::add, Records::addAll);
                    .collect(Collectors.toCollection(Records::new));

            records = fillRecords(
                    needRelateRecords,
                    relation,
                    getRelationRecords(
                            getLocalValues(needRelateRecords, relation),
                            relation),
                    relationKeys.get(relationOffset)
            );

            loadedRelations.put(key, records);
        }

        return records;
    }

    private Records fillRecords(@NotNull List<Record> originalRecords, River.Relation relation, Records relationRecords, String relationKey)
    {
        Record nullRecord = Record.createNull(relation.tableName, relation.getColumns());
        nullRecord.setInserted();

        Records records = new Records();

        Record relationRecord;

        for (Record record: originalRecords
        ) {

            // If record is deleted, add a nullRecord to relations
            Object val = record.get(relation.local);

            if (val == null || relationRecords == null) {

                relationRecord = nullRecord;

            } else {

                relationRecord = relationRecords.find(relation.foreign, val);

                if (relationRecord == null) relationRecord = nullRecord;
            }

            records.addUnique(relationRecord);

            record.with(relationKey, relationRecord);
        }

        return records;
    }

    @Nullable
    private Records getRelationRecords(List<String> localValues, River.Relation relation)
    {
        if (localValues == null || localValues.isEmpty())
            return null;

        Records records = dbFactory.getMySql().getLocalQuery().queryIn(relation, relation.foreign, localValues);
        if (records != null && !records.isEmpty())
            records.forEach(Record::setInserted);

        return records;
    }

    @Nullable
    private List<String> getLocalValues(List<Record> originalRecords, River.Relation relation)
    {
        if (originalRecords == null || originalRecords.isEmpty()) return null;

        Set<String> localValues = new HashSet<>();

        for (Record record: originalRecords
        ) {
            // If record is deleted, return a null to localValues
            Object object = record.get(relation.local);
            if (object == null)
                continue;

            localValues.add(object.toString());
        }

        return localValues.isEmpty() ? null : Arrays.asList(localValues.toArray(new String[0]));
    }
}
