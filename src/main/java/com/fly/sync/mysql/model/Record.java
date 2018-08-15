package com.fly.sync.mysql.model;


import alexh.weak.Dynamic;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.sync.setting.River;
import com.google.common.collect.Sets;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Record {

    public static boolean NESTED = true;
    public final static String DOT = ".";

    public String table;
    protected Map<String, Object> items;
    protected Set<String> modifiedColumns = new HashSet<>();

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
        return NESTED ? Dynamic.from(items).dget(key).isPresent() : items.containsKey(key);
    }

    public Object get(@NotNull String key)
    {
        return containsKey(key) ? (NESTED ? Dynamic.from(items).dget(key).asObject() : items.get(key)) : null;
    }

    public Object get(@Nullable String relationKey, String key)
    {
        return get(null == relationKey || relationKey.isEmpty() ? key : relationKey + "." + key);
    }

    public Record put(@NotNull String key, Object val)
    {
        return set(key, val);
    }

    public Record set(@NotNull String key, Object val)
    {
        if (NESTED)
        {
            String[] segments = key.split(Pattern.quote(DOT));

            Object obj = items;
            int i = 0;
            for (; i < segments.length - 1; ) {
                if (!(obj instanceof Map))
                    obj = new HashMap<String, Object>();

                if (!((Map) obj).containsKey(segments[i]))
                    ((Map)obj).put(segments[i], new HashMap<String, Object>());

                obj = ((Map)obj).get(segments[i]);

                i++;
            }

            ((Map)obj).put(segments[i], val);
        } else
            items.put(key, val);

        return this;
    }

    private static Stream<String> dots(@NotNull Map.Entry entry, @Nullable final String parentKey)
    {
        Object obj = entry.getValue();

        String _parentKey = (parentKey == null ? "" : parentKey + DOT ) + entry.getKey().toString();

        if (!(obj instanceof Map<?, ?>))
            return Stream.of(_parentKey);
        else
            return ((Map<?, ?>) obj)
                    .entrySet().stream().flatMap(entry1 -> Record.dots(entry1, _parentKey));
    }

    private static Stream<String> dots(@NotNull Map.Entry entry)
    {
        return dots(entry, null);
    }

    public Set<String> keys()
    {
        return NESTED ? Sets.newHashSet(items.entrySet().stream().flatMap(Record::dots).iterator()) : items.keySet();
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

    public void with(@NotNull String relationKey, @NotNull Map<String, Object> kv)
    {
        Record record = Record.create(null, kv);
        with(relationKey, record);
    }

    public void with(@NotNull String relationKey, @NotNull Record record)
    {
        for (String key : record.keys()
        ) {
            set(relationKey + DOT + key, record.get(key));
        }
    }

    public Record setModifiedColumn(String columnName)
    {
        modifiedColumns.add(columnName);

        return this;
    }

    public Record setModifiedColumns(List<String> columnNames)
    {
        modifiedColumns.addAll(columnNames);

        return this;
    }

    public Record setTable(String tableName) {
        table = tableName;
        return this;
    }

    public String getTable() {
        return table;
    }

    public Map<String, Object> getItems() {
        return items;
    }

    public Set<String> getModifiedColumns() {
        return modifiedColumns;
    }

    public String toJson(ObjectMapper objectMapper) throws Exception
    {
        return objectMapper
                .writeValueAsString(items);
    }
}
