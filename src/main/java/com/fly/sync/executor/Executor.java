package com.fly.sync.executor;

import com.fly.sync.Main;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalException;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.jdbi.v3.core.statement.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Executor {

    private Es es;
    private MySql mySql;
    private static AtomicBoolean running = new AtomicBoolean(false);
    public final static Logger logger = LoggerFactory.getLogger(Main.class);
    private ExecutorService threadPool;

    public Executor()
    {
        threadPool = Executors.newFixedThreadPool(Setting.river.databases.size() * 10, new ThreadFactoryBuilder().setNameFormat("SubscribeOn-%d").build());
        es = new Es(Setting.river, true);
        mySql = new MySql(Setting.river, true);
    }

    public void connect() throws Exception
    {
        es.connect();
        logger.info("Connect to Elasticsearch.");
        mySql.connect();
        logger.info("Connected to MySQL.");

    }

    public void validate() throws Exception
    {
        mySql.validate();
    }

    public void close()
    {
        try {
            es.close();
            mySql.close();
        } catch (Exception e)
        {

        }
    }

    public void run()
    {
        if (running.get())
            throw new RejectedExecutionException("Application is running.");

        running.set(true);

        Scheduler scheduler = Schedulers.from(threadPool);

        for (River.Database database: Setting.river.databases
                     ) {
            new Emiter(this, database)
                .buildObservable(scheduler)
                //.compose(FlowableBufferTimed.build(Setting.config.flushBulkTime, TimeUnit.MILLISECONDS, scheduler, Setting.config.bulkSize > 120 ? 120 : Setting.config.bulkSize))
                .subscribeOn(scheduler)
                .observeOn(scheduler)
                .subscribe(new Consumer(this, database));
        }
    }

    public void await() throws InterruptedException
    {
        while(isRunning())
        {
            Thread.sleep(100);
        }
    }

    public void throwException(Throwable e)
    {
        synchronized (Executor.class)
        {
            logger.error(e.getMessage(), e);
            if (e instanceof FatalException)
            {
                stop();
            }
        }
    }

    public Es getEs() {
        return es;
    }

    public MySql getMySql() {
        return mySql;
    }

    public static boolean isRunning() {
        return running.get();
    }

    public void stop() {
        running.set(false);
        threadPool.shutdown();
    }
}
