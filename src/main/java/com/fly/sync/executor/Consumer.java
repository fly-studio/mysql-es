package com.fly.sync.executor;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractRecordAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.River;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Consumer implements Observer<List<AbstractAction>>, DbFactory {
    public final static Logger logger = LoggerFactory.getLogger(Consumer.class);

    private Executor executor;
    private River.Database database;
    private Disposable disposable;

    public Consumer(Executor executor, River.Database database) {
        this.executor = executor;
        this.database = database;
    }

    @Override
    public Es getEs()
    {
        return executor.getEs();
    }

    @Override
    public MySql getMySql()
    {
        return executor.getMySql();
    }

    @Override
    public River.Database getRiverDatabase() {
        return database;
    }

    @Override
    public Statistic getStatistic() {
        return executor.getStatistic();
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void onSubscribe(Disposable disposable) {
        this.disposable = disposable;
    }

    @Override
    public void onNext(List<AbstractAction> actionList) {

        if (actionList.isEmpty())
            return;

        getStatistic().getSubscribeCount().addAndGet(actionList.size());

        // group by records
        if (!(actionList.get(0) instanceof AbstractRecordAction))
        {
            for (AbstractAction action: actionList
                 ) {
                action.execute(this);
            }

            return;
        }

        executeRecords(actionList);
    }

    private void executeRecords(List<AbstractAction> actionList)
    {
        if (!(actionList.get(0) instanceof AbstractRecordAction))
            return;

        long nanoTime = System.nanoTime();

        getStatistic().getRecordCount().addAndGet(actionList.size());

        BulkRequest request = new BulkRequest();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);

        try {

            for (AbstractAction action : actionList
            ) {
                request.add(
                        ((AbstractRecordAction)action).getRequest(this)
                );
            }

        } catch (Exception e)
        {
            logger.error(e.getMessage(), e);
            return;
        }

        try {

            BulkResponse bulkResponse = getEs().getClient().bulk(request);
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

    @Override
    public void onError(Throwable throwable) {
        executor.throwException(throwable);
    }

    @Override
    public void onComplete() {
        disposable.dispose();
        executor.stop();
    }
}
