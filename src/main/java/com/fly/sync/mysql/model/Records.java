package com.fly.sync.mysql.model;

import com.fly.core.text.json.Jsonable;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.setting.River;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Records extends ArrayList<Record> implements AbstractAction {

    public static <T> Records create(List<T> objects)
    {
        Records records = new Records();

        for (T o: objects
            ) {
            if (o instanceof Record)
                records.add((Record)o);
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


    public void execute(DbFactory dbFactory)
    {
        BulkRequest request = new BulkRequest();
        try {

            for (Record record : this
            ) {
                River.Table table = dbFactory.getRiverDatabase().getTable(record.table);
                request.add(
                        new IndexRequest(table.index, table.type, record.getID(table))
                                .source(record.toJson(Jsonable.Builder.makeAdapter()), XContentType.JSON)
                );
            }
        } catch (Exception e)
        {
            logger.error(e.getMessage(), e);
        }

        try {

            BulkResponse bulkResponse = dbFactory.getEs().getClient().bulk(request);
            for (BulkItemResponse response : bulkResponse
            ) {
                if (response.isFailed())
                    logger.error(response.getFailureMessage());
            }
        } catch (IOException e) {
            logger.error("Elasticsearch request failed.", e);
        }

    }
}
