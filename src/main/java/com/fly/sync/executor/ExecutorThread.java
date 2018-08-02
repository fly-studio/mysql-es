package com.fly.sync.executor;

import com.fly.sync.contract.DatabaseListener;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalEsException;
import com.fly.sync.exception.OutOfRetryException;
import com.fly.sync.mysql.Dumper;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.schedulers.Schedulers;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ExecutorThread extends Thread {

    private River.Database database;
    private Executor executor;
    private DatabaseListener listener;


    public ExecutorThread(Executor executor, River.Database database) {
        super();

        this.executor = executor;
        this.database = database;
        listener = new On(this);

    }

    public Es getEs()
    {
        return executor.getEs();
    }

    public MySql getMySql()
    {
        return executor.getMySql();
    }

    public River.Database getDatabase() {
        return database;
    }

    public Executor getExecutor() {
        return executor;
    }

    public DatabaseListener getListener() {
        return listener;
    }

    @Override
    public void run() {

        createIndices();

        runDumper();

        while (!isInterrupted()){ //非阻塞过程中通过判断中断标志来退出
            try{

                if (runCanal() <= 0)
                    Thread.sleep(Setting.config.flushBulkTime);

            } catch (InterruptedException e){

                break;//捕获到异常之后，执行break跳出循环。
            }
        }
    }

    private void createIndices()
    {
        try {
            this.getEs().createIndices(database, Setting.binLog.isEmpty(database.db));
        } catch (IOException e)
        {
            throwException(new FatalEsException("Create Indices of DB: \"" + database.db + "\" Failed.", e));
        } catch (OutOfRetryException e)
        {
            throwException(e);
        }
    }

    private void runDumper()
    {
        if (!Setting.binLog.isEmpty(database.db))
            return;

        Dumper dumper = new Dumper(Setting.config, Setting.river, database, listener);

        dumper.run();

    }

    private int runCanal()
    {
        System.out.println("run Cannal.");
        return 0;
    }

    public void throwException(Exception e)
    {
        executor.throwException(this, e);
    }


}
