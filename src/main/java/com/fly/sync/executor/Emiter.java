package com.fly.sync.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.sync.Main;
import com.fly.sync.action.ReportAction;
import com.fly.sync.canal.Canal;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractRecordAction;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Emiter implements DbFactory {

    public final static Logger logger = LoggerFactory.getLogger(Emiter.class);
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

    @Override
    public ObjectMapper getJsonMapper() {
        return executor.getJsonMapper();
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
                .subscribeOn(scheduler)
                .observeOn(scheduler)
                .buffer(Setting.config.flushBulkTime, TimeUnit.MILLISECONDS, scheduler, Setting.config.bulkSize)

                .observeOn(scheduler)
                .concatMap(new WithGroup())

                .observeOn(scheduler)
                .map(new WithRelations())

                .observeOn(scheduler)
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

        return Observable.create( emitter -> {
            while(Setting.binLog.isEmpty(database.schemaName))
            {
                try {

                    Thread.sleep(500);
                } catch (InterruptedException e)
                {

                }
            }
            dumper = null;
            emitter.onNext(Main.NAME);
            emitter.onComplete();

        }).flatMap(nothing -> {

            if (Setting.binLog.isEmpty(database.schemaName))
                return Observable.empty();

            canal = new Canal(Setting.config, Setting.river, Setting.binLog.get(getRiverDatabase().schemaName), this);

            return canal.run(scheduler);
        });
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
                if (actionList.isEmpty())
                    return Observable.empty();


                lists.clear();
                actions.clear();

                for (AbstractAction action : actionList
                ) {
                    add(action);
                }

                emit();

                List<Integer> counts = lists.stream().map(actions -> actions.size()).collect(Collectors.toList());

                logger.trace("WithGroup Total: {}, split to {}", actionList.size(), counts);

                return Observable.fromIterable(lists);
            }

        }
    }

    public class WithRelations implements Function<List<AbstractAction>, List<AbstractAction>> {

        @Override
        public List<AbstractAction> apply(List<AbstractAction> actionList) throws Exception {
            logger.trace("WithRelations: {}", actionList.size());

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
