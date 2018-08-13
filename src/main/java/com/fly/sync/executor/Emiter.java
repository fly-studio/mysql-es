package com.fly.sync.executor;

import com.fly.sync.action.ReportAction;
import com.fly.sync.canal.Canal;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractRecordAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalCanalException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Emiter implements DbFactory {

    private Executor executor;
    private River.Database database;
    Canal canal;
    Dumper dumper;

    public Emiter(Executor executor, River.Database database) {
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

    public Observable<List<AbstractAction>> buildObservable(Scheduler scheduler) {

        createIndices();

        return Observable.merge(
                    Observable.concat(
                        runDumper(scheduler),
                        runCanal(scheduler)
                    ),
                Observable.interval(15, TimeUnit.SECONDS)
                        .subscribeOn(scheduler)
                        .observeOn(scheduler)
                        .map(v -> new ReportAction())
            )
            .takeWhile(value -> executor.isRunning())

                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .buffer(Setting.config.flushBulkTime, TimeUnit.MILLISECONDS, scheduler, Setting.config.bulkSize)

                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .concatMap(new WithGroup())

                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .map(new WithRelations())

                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .map(new AliasColumns())
            ;
    }

    private void createIndices()
    {
        if (!Setting.binLog.isEmpty(database.schemaName))
            return;

        try {
            this.getEs().createIndices(database);
        } catch (IOException e) {
            throw new FatalEsException("Create Indices of DB: \"" + database.schemaName + "\" Failed.", e);
        }
    }

    private Observable<AbstractAction> runDumper(Scheduler scheduler)
    {
        if (!Setting.binLog.isEmpty(database.schemaName))
            return Observable.empty();

        dumper = new Dumper(Setting.config, Setting.river, this);

        return dumper.run(scheduler);
    }

    private Observable<AbstractAction> runCanal(Scheduler scheduler) {

        dumper = null;

        if (Setting.binLog.isEmpty(database.schemaName))
            throw new FatalCanalException("BinLog position lost.");

        canal = new Canal(Setting.config, Setting.river, Setting.binLog.get(getRiverDatabase().schemaName), this);

        return canal.run(scheduler);
    }

    public class WithGroup implements Function<List<AbstractAction>, Observable<List<AbstractAction>>>
    {
        private String lastGroup;
        private List<List<AbstractAction>> lists = new ArrayList<>();
        private List<AbstractAction> actions = new ArrayList<>();

        public void add(AbstractAction action)
        {
            if (!action.getGroup().equals(lastGroup))
                emit();

            lastGroup = action.getGroup();
            actions.add(action);
        }

        public void emit()
        {
            if (actions.isEmpty())
                return;

            lists.add(Arrays.asList(actions.toArray(new AbstractAction[0])));
            lastGroup = null;
            actions.clear();
        }

        @Override
        public Observable<List<AbstractAction>> apply(List<AbstractAction> actionList) throws Exception {

            synchronized (this)
            {
                lists.clear();
                actions.clear();

                for (AbstractAction action : actionList
                ) {
                    add(action);
                }

                emit();
                return Observable.fromIterable(lists);
            }

        }
    }

    public class WithRelations implements Function<List<AbstractAction>, List<AbstractAction>> {

        @Override
        public List<AbstractAction> apply(List<AbstractAction> actionList) throws Exception {

            if (actionList.isEmpty() || !(actionList.get(0) instanceof AbstractRecordAction))
                return actionList;

            Relation relation = new Relation(Emiter.this, actionList);
            relation.load();

            return actionList;
        }
    }

    public class AliasColumns implements Function<List<AbstractAction>, List<AbstractAction>> {

        @Override
        public List<AbstractAction> apply(List<AbstractAction> actionList) throws Exception {
            //Todo

            return actionList;
        }
    }


}
