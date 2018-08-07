package com.fly.sync.mysql.model;

import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.util.ArrayList;

public class Records extends ArrayList<Record> {

    @NotNull
    public Records search(@NotNull String key, @Nullable Object val)
    {
        Records results = new Records();
        for (Record record: this
        ) {
            if (record.equals(key, val))
                results.add(record);
        }
        return results;
    }

    @Nullable
    public Record find(@NotNull String key, @Nullable Object val)
    {
        for (Record record: this
        ) {
            if (record.equals(key, val))
                return record;
        }
        return null;
    }

}
