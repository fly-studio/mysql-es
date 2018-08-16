package com.fly.sync.mysql.model;


import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.sync.setting.River;
import com.google.common.collect.Sets;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.util.*;

public class Record {

    public String table;
    private Map<String, Object> items;
    private Map<String, Record> relations = new HashMap<>();
    private Set<String> modifiedColumns = new HashSet<>();
    private CanalEntry.EventType eventType = null;

    public Record(String table, Map<String, Object> items) {
        this.table = table;
        this.items = items;
    }

    public static <T extends Object> Record create(String table, List<String> columns, List<T> itemList) {

        if (columns.size() != itemList.size())
            throw new ArrayIndexOutOfBoundsException("columnNames's size MUST equal to valueList's size.");

        Map<String, Object> kv = new HashMap<>();
        Record record = new Record(table, kv);

        for (int i = 0; i < columns.size(); i++)
            record.set(columns.get(i), itemList.get(i));

        return record;
    }

    public static Record create(String table, Map<String, Object> kv)
    {
        return new Record(table, kv);
    }

    public static Record createNull(String table, List<String> columns)
    {
        Map<String, Object> kv = new HashMap<>();
        Record record = new Record(table, kv);

        for (int i = 0; i < columns.size(); i++)
            record.set(columns.get(i), null);

        return record;
    }

    public Record setEventType(CanalEntry.EventType eventType) {
        this.eventType = eventType;

        return this;
    }

    public Record setInserted()
    {
        setAllModifiedColumns();

        return setEventType(CanalEntry.EventType.INSERT);
    }

    public Record setDeleted()
    {
        setAllModifiedColumns();

        return setEventType(CanalEntry.EventType.DELETE);
    }

    public Record setUpdated(List<String> columnNames)
    {

        setModifiedColumns(columnNames);

        return setEventType(CanalEntry.EventType.UPDATE);
    }

    public boolean setModifiedColumn(String columnName)
    {
        if (items.containsKey(columnName)) {
            modifiedColumns.add(columnName);
            return true;
        }

        return false;
    }

    public boolean setAllModifiedColumns()
    {
        modifiedColumns.clear();
        modifiedColumns.addAll(keys());

        return true;
    }

    public boolean setModifiedColumns(List<String> columnNames)
    {
        Set<String> keys = Sets.newHashSet(items.keySet());

        keys.retainAll(columnNames);

        modifiedColumns.addAll(keys);

        return !keys.isEmpty();
    }

    public Record setTable(String tableName) {
        table = tableName;
        return this;
    }

    public boolean equals(@NotNull String key, @Nullable Object val, boolean strict)
    {
        if (containsKey(key))
        {
            Object obj = get(key);

            if (val == null)
                return obj == null;

            return strict ? val.equals(obj) : val.toString().equalsIgnoreCase(obj.toString());
        }

        return false;
    }

    public boolean equals(@NotNull String key, @Nullable Object val)
    {
        return equals(key, val, false);
    }

    public boolean containsKey(@NotNull String key)
    {
        return items.containsKey(key);
    }

    public Object get(@NotNull String key)
    {
        return items.get(key);
    }

    public Object get(String relationKey, String key)
    {
        if (relationKey == null || relationKey.isEmpty())
            return get(key);

        Record record = getRelation(relationKey);

        return record == null ? null : record.get(key);
    }

    public Record getRelation(@Nullable String relationKey)
    {
        return relations.get(relationKey);
    }

    public Record put(@NotNull String key, Object val)
    {
        return set(key, val);
    }

    public Record set(@NotNull String key, Object val)
    {
        items.put(key, val);

        return this;
    }

    public Set<String> keys()
    {
        return items.keySet();
    }

    public String getID(River.Table table)
    {
        if (table.pk.size() == 1)
            return get(table.pk.get(0)).toString();

        StringJoiner sj = new StringJoiner(":");
        for (String key: table.pk
             ) {
            sj.add(get(key).toString());
        }

        return sj.toString();
    }


    public void with(@NotNull String relationKey, @NotNull Record record)
    {
        relations.put(relationKey, record);
    }

    public String getTable() {
        return table;
    }

    public boolean isDeleted()
    {
        return eventType == CanalEntry.EventType.DELETE;
    }

    public boolean isUpdated()
    {
        return eventType == CanalEntry.EventType.UPDATE;
    }

    public boolean isInserted()
    {
        return eventType == CanalEntry.EventType.INSERT;
    }

    public Map<String, Object> getItems() {
        return items;
    }

    public Set<String> getModifiedColumns() {
        return modifiedColumns;
    }

    public boolean isModified(String columnName)
    {
        return modifiedColumns.contains(columnName);
    }

    public Map<String, Record> getRelations() {
        return relations;
    }

    public Map<String, Object> getModifiedItems()
    {
        if (modifiedColumns.size() == items.keySet().size())
            return items;

        Map<String, Object> newItems = new HashMap<>();

        for (String key: modifiedColumns
        )
            newItems.put(key, isDeleted() ? null : get(key));

        return newItems;
    }

    public Map<String, Object> mix(boolean nested)
    {
        Map<String, Object> newItems = getModifiedItems();

        for (Map.Entry<String, Record> entry: relations.entrySet()
        ) {
            String relationKey = entry.getKey();
            Map<String, Object> value = entry.getValue().mix(nested);

            if (nested)
            {
                newItems.put(relationKey, value);
            } else {
                for (Map.Entry<String, Object> entry1: value.entrySet()
                ) {
                    newItems.put(relationKey + River.DOT + entry1.getKey(), entry1.getValue());
                }
            }
        }

        return newItems;
    }

    public Map<String, Object> mix()
    {
        return mix(false);
    }

    public String toJson(ObjectMapper objectMapper, boolean nested) throws Exception
    {
        Map<String, Object> mix = mix(nested);

        return objectMapper
                .writeValueAsString(mix);
    }

    public String toJson(ObjectMapper objectMapper) throws Exception
    {
        return toJson(objectMapper, false);
    }

}
