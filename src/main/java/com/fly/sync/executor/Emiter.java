package com.fly.sync.executor;

import com.fly.sync.action.NullAction;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalEsException;
import com.fly.sync.mysql.Dumper;
import com.fly.sync.mysql.MySql;
import com.fly.sync.mysql.Relation;
import com.fly.sync.mysql.model.Record;
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
            .takeWhile(value -> executor.isRunning())
            .buffer(Setting.config.flushBulkTime, TimeUnit.MILLISECONDS, scheduler, Setting.config.bulkSize)
            .concatMap(new SplitRecordActions())
            .map(new WithRelations())
            .map(new AliasColumns())
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
        if (!Setting.binLog.isEmpty(database.schemaName))
            return;

        try {
            this.getEs().createIndices(database, Setting.binLog.isEmpty(database.schemaName));
        } catch (IOException e) {
            throw new FatalEsException("Create Indices of DB: \"" + database.schemaName + "\" Failed.", e);
        }
    }

    private Observable<AbstractAction> runDumper(Scheduler scheduler)
    {
        //if (!Setting.binLog.isEmpty(database.schemaName))
         //   return Observable.empty();

        Dumper dumper = new Dumper(Setting.config, Setting.river, this);

        return dumper.run(scheduler);
    }

    public class SplitRecordActions implements Function<List<AbstractAction>, Observable<List<AbstractAction>>>
    {
        @Override
        public Observable<List<AbstractAction>> apply(List<AbstractAction> actionList) throws Exception {
            List<List<AbstractAction>> lists = new ArrayList<>();
            List<AbstractAction> actions = new ArrayList<>();
            for (AbstractAction action : actionList
            ) {
                if (!(action instanceof Record))
                {
                    // set ChangePositionAction/Other into a singe
                    if (!actions.isEmpty()) lists.add(actions);
                    lists.add(Arrays.asList(action));

                    actions = new ArrayList<>();
                    continue;
                }
                actions.add(action);
            }
            // last
            if (!actions.isEmpty()) lists.add(actions);
            return Observable.fromIterable(lists);
        }
    }

    public class WithRelations implements Function<List<AbstractAction>, List<AbstractAction>> {

        @Override
        public List<AbstractAction> apply(List<AbstractAction> actionList) throws Exception {

            if (actionList.size() == 1 && !(actionList.get(0) instanceof Record))
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
