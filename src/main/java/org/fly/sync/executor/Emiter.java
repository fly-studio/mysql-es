package org.fly.sync.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.fly.sync.action.ReportAction;
import org.fly.sync.canal.Canal;
import org.fly.sync.contract.AbstractAction;
import org.fly.sync.contract.AbstractLifeCycle;
import org.fly.sync.contract.AbstractRecordAction;
import org.fly.sync.contract.DbFactory;
import org.fly.sync.es.Es;
import org.fly.sync.exception.EsFatalException;
import org.fly.sync.mysql.Dumper;
import org.fly.sync.mysql.MySql;
import org.fly.sync.mysql.relation.Relation;
import org.fly.sync.setting.River;
import org.fly.sync.setting.Setting;
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

public class Emiter extends AbstractLifeCycle implements DbFactory {

    public final static Logger logger = LoggerFactory.getLogger(Emiter.class);
    private Executor executor;
    private River.Database database;
    private Canal canal = null;
    private Dumper dumper = null;

    public Emiter(Executor executor, River.Database database) {
        this.executor = executor;
        this.database = database;

    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();

        if (null != dumper)
            dumper.stop();

        if (null != canal)
            canal.stop();
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
            throw new EsFatalException("Create Indices of DB: \"" + database.schemaName + "\" Failed.", e);
        }
    }

    private Observable<AbstractAction> runDumper(Scheduler scheduler)
    {
        if (!Setting.binLog.isEmpty(database.schemaName))
            return Observable.empty();

        dumper = new Dumper(Setting.config, Setting.river, this);
        dumper.start();

        return dumper.run(scheduler)
                .doOnComplete(() -> {
                    dumper.stop();
                    dumper = null;
                });
    }

    private Observable<AbstractAction> runCanal(Scheduler scheduler) {

        return Observable.create( emitter -> {
            while(Setting.binLog.isEmpty(database.schemaName))
            {
                try {

                    Thread.sleep(500);
                } catch (InterruptedException e)
                {
                    emitter.onComplete();
                    return;
                }
            }

            emitter.onNext("Complete");
            emitter.onComplete();

        }).flatMap(nothing -> {

            if (Setting.binLog.isEmpty(database.schemaName))
                return Observable.empty();

            canal = new Canal(Setting.config, Setting.river, Setting.binLog.get(getRiverDatabase().schemaName), this);
            canal.start();

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
        public synchronized Observable<List<AbstractAction>> apply(List<AbstractAction> actionList) throws Exception {
            if (actionList.isEmpty())
                return Observable.empty();

            lists.clear();
            actions.clear();

            for (AbstractAction action : actionList
            ) {
                add(action);
            }

            emit();

            List<Integer> counts = lists.stream().map(List::size).collect(Collectors.toList());

            logger.trace("WithGroup Total: {}, split to {}", actionList.size(), counts);

            return Observable.fromIterable(lists);
        }
    }

    public class WithRelations implements Function<List<AbstractAction>, List<AbstractAction>> {

        @Override
        public List<AbstractAction> apply(List<AbstractAction> actionList) throws Exception {

            if (actionList.isEmpty() || !(actionList.get(0) instanceof AbstractRecordAction)) {
                return actionList;
            }

            logger.trace("WithRelations: {}", actionList.size());

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
