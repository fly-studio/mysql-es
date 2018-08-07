package com.fly.sync.mysql.model;


import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Record {

    public String table;
    public Map<String, Object> items;

    public Record(String table, Map<String, Object> items) {
        this.table = table;
        this.items = items;
    }

    public static Record create(String table, List<String> columns, List<Object> itemList) {

        if (columns.size() != itemList.size())
            throw new ArrayIndexOutOfBoundsException("columns's size MUST equal to valueList's size.");

        Map<String, Object> kv = new HashMap<>();
        for (int i = 0; i < columns.size(); i++)
            kv.put(columns.get(i), itemList.get(i));

        return new Record(table, kv);
    }

    public static Record create(String table, Map<String, Object> kv)
    {
        return new Record(table, kv);
    }

    public static Record create(String table, List<String> columns)
    {
        Map<String, Object> value = new HashMap<>();
        for (int i = 0; i < columns.size(); i++)
            value.put(columns.get(i), null);

        return new Record(table, value);
    }

    public boolean equals(@NotNull String key, @Nullable Object val)
    {
        if (items.containsKey(key))
        {
            Object obj = get(key);

            if (val == null)
                return obj == null;

            return val.equals(obj);
        }

        return false;
    }

    public Object get(@NotNull String key)
    {
        return items.get(key);
    }

    public Object get(@Nullable String relationKey, String key)
    {
        return items.get(null == relationKey || relationKey.isEmpty() ? key : relationKey + "." + key);
    }

    public Record set(@NotNull String key, Object val)
    {
        items.put(key, val);
        return this;
    }

    public void with(@NotNull String relationKey, @NotNull Map<String, Object> kv)
    {
        for (Map.Entry<String, Object> entry : kv.entrySet()
        ) {
            items.put(relationKey + "." + entry.getKey(), entry.getValue());
        }
    }

    public void with(@NotNull String relationKey, @NotNull Record record)
    {
        for (Map.Entry<String, Object> entry : record.items.entrySet()
        ) {
            items.put(relationKey + "." + entry.getKey(), entry.getValue());
        }
    }

    public Record setTable(String tableName) {
        table = tableName;
        return this;
    }
}
