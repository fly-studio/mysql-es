package com.fly.sync.mysql.model;

import com.fly.sync.contract.AbstractRecordAction;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Records extends ArrayList<Record> {
    public final static Logger logger = LoggerFactory.getLogger(Records.class);

    public static Records create(List<AbstractRecordAction> recordActions)
    {
        Records records = new Records();
        for (AbstractRecordAction action: recordActions
             ) {
            records.add(action.getRecord());
        }

        return records;
    }

    @NotNull
    public Records search(@NotNull String key, @Nullable Object val, boolean strict)
    {
        Records results = new Records();
        for (Record record: this
        ) {
            if (record.equals(key, val, strict))
                results.add(record);
        }
        return results;
    }

    @Nullable
    public Records search(@NotNull String key, @Nullable Object val)
    {
        return search(key, val, false);
    }

    @Nullable
    public Record find(@NotNull String key, @Nullable Object val, boolean strict)
    {
        for (Record record: this
        ) {
            if (record.equals(key, val, strict))
                return record;
        }
        return null;
    }

    @Nullable
    public Record find(@NotNull String key, @Nullable Object val)
    {
        return find(key, val, false);
    }

}
