package org.fly.sync.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.fly.sync.action.DeleteAction;
import org.fly.sync.contract.*;
import org.fly.sync.es.Es;
import org.fly.sync.es.Writer;
import org.fly.sync.mysql.MySql;
import org.fly.sync.setting.River;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class Consumer extends AbstractLifeCycle implements Observer<List<AbstractAction>>, DbFactory {

    public final static Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final static List<String> bulkGroup = Arrays.asList(
            AbstractRecordAction.class.getName(),
            DeleteAction.class.getName()
    );

    private Executor executor;
    private River.Database database;
    private Disposable disposable;
    private Writer writer;

    public Consumer(Executor executor, River.Database database) {
        this.executor = executor;
        this.database = database;
        writer = new Writer(this);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();

        if (null != disposable && !disposable.isDisposed())
            disposable.dispose();
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

    @Override
    public ObjectMapper getJsonMapper() {
        return executor.getJsonMapper();
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void onSubscribe(Disposable disposable) {
        this.disposable = disposable;
    }

    @Override
    public synchronized void onNext(List<AbstractAction> actionList) {

        if (actionList.isEmpty())
            return;

        getStatistic().getSubscribeCount().addAndGet(actionList.size());
        getStatistic().getRecordCount().addAndGet(actionList.stream().filter(action -> action instanceof AbstractRecordAction).count());
        getStatistic().getRelateCount().addAndGet(actionList.stream().filter(action -> action instanceof AbstractRelateAction).count());

        try
        {
            // bulk
            if (bulkGroup.contains(actionList.get(0).getGroup()))
            {
                writer.executeBulk(actionList);
            } else {
                writer.execute(actionList);
            }
        } catch (Exception e) {
            disposable.dispose();
            onError(e);
        }
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
