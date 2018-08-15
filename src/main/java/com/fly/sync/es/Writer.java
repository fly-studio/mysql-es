package com.fly.sync.es;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractRecordAction;
import com.fly.sync.contract.DbFactory;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Writer {
    public final static Logger logger = LoggerFactory.getLogger(Writer.class);

    DbFactory dbFactory;

    public Writer(DbFactory dbFactory) {
        this.dbFactory = dbFactory;
    }

    public void execute(List<AbstractAction> actionList)
    {
        for (AbstractAction action: actionList
        ) {
            action.execute(dbFactory);
        }
    }

    public void executeBulk(List<AbstractAction> actionList)
    {
        long nanoTime = System.nanoTime();

        BulkRequest request = new BulkRequest();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);

        try {

            for (AbstractAction action : actionList
            ) {
                action.execute(dbFactory);
                request.add(
                        ((AbstractRecordAction)action).getRequest(dbFactory)
                );
            }

        } catch (Exception e)
        {
            logger.error(e.getMessage(), e);
            return;
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

        logger.info("Sync {} records, duration: {} ms", actionList.size(), (System.nanoTime() - nanoTime) / 1000_000.0);
    }
}
