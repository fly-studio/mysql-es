package com.fly.sync.executor;

import com.fly.sync.action.RecordsAction;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractRecord;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.River;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        if (actionList.size() > 0 && !(actionList.get(0) instanceof AbstractRecord))
        {
            for (AbstractAction action: actionList
                 ) {
                action.execute(this);
            }

            return;
        }

        RecordsAction recordsAction = RecordsAction.create(actionList);
        recordsAction.execute(this);
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
