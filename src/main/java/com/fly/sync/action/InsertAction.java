package com.fly.sync.action;

import com.fly.sync.contract.AbstractAction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertAction implements AbstractAction {

    public String table;
    public Map<String, Object> value;

    public InsertAction(String table, Map<String, Object> value) {
        this.table = table;
        this.value = value;
    }

    public static InsertAction create(String table, List<String> columns, List<Object> valueList) {
        if (columns.size() != valueList.size())
            throw new ArrayIndexOutOfBoundsException("columns's size MUST equal to valueList's size.");

        Map<String, Object> value = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            value.put(columns.get(i), valueList.get(i));
        }
        return new InsertAction(table, value);
    }

    public Object find(String key)
    {
        return value.get(key);
    }

    public void with(String relationKey, Map<String, Object> kv)
    {
        for (Map.Entry<String, Object> entry : kv.entrySet()
             ) {
            value.put(relationKey + "." + entry.getKey(), entry.getValue());
        }
    }
}
