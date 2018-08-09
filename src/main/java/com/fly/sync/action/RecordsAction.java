package com.fly.sync.action;

import com.fly.core.text.json.Jsonable;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractRecord;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.mysql.model.Record;
import com.fly.sync.setting.River;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecordsAction extends ArrayList<Record> implements AbstractAction {
    public final static Logger logger = LoggerFactory.getLogger(RecordsAction.class);

    public static <T> RecordsAction create(List<T> objects)
    {
        RecordsAction recordsAction = new RecordsAction();

        for (T o: objects
            ) {
            if (o instanceof AbstractRecord)
                recordsAction.add(((AbstractRecord) o).getRecord());
        }

        return recordsAction;
    }

    @NotNull
    public RecordsAction search(@NotNull String key, @Nullable Object val, boolean strict)
    {
        RecordsAction results = new RecordsAction();
        for (Record record: this
        ) {
            if (record.equals(key, val, strict))
                results.add(record);
        }
        return results;
    }

    @Nullable
    public RecordsAction search(@NotNull String key, @Nullable Object val)
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
        long nanoTime = System.nanoTime();

        dbFactory.getStatistic().getRecordCount().addAndGet(size());

        BulkRequest request = new BulkRequest();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);

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

        logger.info("Execute {} records, duration: {} ms", size(), (System.nanoTime() - nanoTime) / 1000_000.0);

    }
}
