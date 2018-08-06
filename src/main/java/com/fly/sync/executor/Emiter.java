package com.fly.sync.executor;

import com.fly.sync.action.NullAction;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalEsException;
import com.fly.sync.mysql.Dumper;
import com.fly.sync.mysql.MySql;
import com.fly.sync.mysql.Relation;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Function;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Emiter implements DbFactory {

    private Executor executor;
    private River.Database database;

    public Emiter(Executor executor, River.Database database) {
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

    public Observable<List<AbstractAction>> buildObservable(Scheduler scheduler) {

        createIndices();

        return Observable.concat(
                runDumper(scheduler),
                runCanal(scheduler)
            )
            .buffer(Setting.config.flushBulkTime, TimeUnit.MILLISECONDS, scheduler, Setting.config.bulkSize)
            .map(new WithRelations())
            ;
    }

    private Observable<AbstractAction> runCanal(Scheduler scheduler) {

        return Observable.interval(1, TimeUnit.SECONDS).map(new Function<Long, AbstractAction>() {
            @Override
            public AbstractAction apply(Long aLong) throws Exception {
                return new NullAction();
            }
        });
    }

    private void createIndices()
    {
        try {
            this.getEs().createIndices(database, Setting.binLog.isEmpty(database.db));
        } catch (IOException e) {
            throw new FatalEsException("Create Indices of DB: \"" + database.db + "\" Failed.", e);
        }
    }

    private Observable<AbstractAction> runDumper(Scheduler scheduler)
    {
        if (!Setting.binLog.isEmpty(database.db))
            return Observable.empty();

        Dumper dumper = new Dumper(Setting.config, Setting.river, this);

        return dumper.run(scheduler);

    }

    public class WithRelations implements Function<List<AbstractAction>, List<AbstractAction>> {

        @Override
        public List<AbstractAction> apply(List<AbstractAction> actionList) throws Exception {

            Relation relation = new Relation(Emiter.this);
            return relation.load(actionList);
        };
    }


}
