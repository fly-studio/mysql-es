package com.fly.sync.executor;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalEsException;
import com.fly.sync.mysql.Dumper;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;

import java.io.IOException;

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

    public Flowable<AbstractAction> buildFlowable(Scheduler scheduler) {

        createIndices();

        return Flowable.concat(
                runDumper(scheduler),
                runCanal(scheduler)
        );
    }

    private Flowable<AbstractAction> runCanal(Scheduler scheduler) {
        return Flowable.empty();
    }

    private void createIndices()
    {
        try {
            this.getEs().createIndices(database, Setting.binLog.isEmpty(database.db));
        } catch (IOException e) {
            throw new FatalEsException("Create Indices of DB: \"" + database.db + "\" Failed.", e);
        }
    }

    private Flowable<AbstractAction> runDumper(Scheduler scheduler)
    {
        if (!Setting.binLog.isEmpty(database.db))
            return Flowable.empty();

        Dumper dumper = new Dumper(Setting.config, Setting.river, this);

        return dumper.run(scheduler);

    }


}
