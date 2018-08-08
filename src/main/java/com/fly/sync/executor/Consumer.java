package com.fly.sync.executor;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.mysql.MySql;
import com.fly.sync.mysql.model.Record;
import com.fly.sync.mysql.model.Records;
import com.fly.sync.setting.River;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;

import java.util.List;

public class Consumer implements Observer<List<AbstractAction>>, DbFactory {
    private Executor executor;
    private River.Database database;

    public Consumer(Executor executor, River.Database database) {
        this.executor = executor;
        this.database = database;
    }

    public Es getEs()
    {
        return executor.getEs();
    }

    public MySql getMySql()
    {
        return executor.getMySql();
    }

    public River.Database getRiverDatabase() {
        return database;
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void onSubscribe(Disposable disposable) {

    }

    @Override
    public void onNext(List<AbstractAction> actionList) {
        if (actionList.size() > 0 && !(actionList.get(0) instanceof Record))
        {
            for (AbstractAction action: actionList
                 ) {
                action.execute(this);
            }
        } else {
            Records records = Records.create(actionList);
            records.execute(this);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        executor.throwException(throwable);
    }

    @Override
    public void onComplete() {
        executor.stop();
    }
}
