package com.fly.sync.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.sync.Main;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalException;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.Scheduler;
import io.reactivex.exceptions.UndeliverableException;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Executor {

    public enum Action {
        CONNECT,
        VALIDATE,
        START,
        STOP,
    }

    private Es es;
    private MySql mySql;
    private static AtomicBoolean running = new AtomicBoolean(false);
    public final static Logger logger = LoggerFactory.getLogger(Main.class);
    private ExecutorService threadPool;
    private Statistic statistic = new Statistic();
    private List<Emiter> emiters = new ArrayList<>();
    private List<Consumer> consumers = new ArrayList<>();

    private LinkedList<Action> queue = new LinkedList<Action>();

    public Executor()
    {
        threadPool = Executors.newFixedThreadPool(Setting.river.databases.size() * 10, new ThreadFactoryBuilder().setNameFormat("SubscribeOn-%d").build());
        es = new Es(Setting.river, true);
        mySql = new MySql(Setting.river, true);
    }

    public void run() throws Exception
    {
        while(true)
        {
            Action action = queue.isEmpty() ? null : queue.pop();

            if (action == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            } else
            {
                switch (action)
                {
                    case START:
                        start();
                        break;
                    case STOP:
                        stop();
                        return; // return run
                    case CONNECT:
                        connect();
                        break;
                    case VALIDATE:
                        validate();
                        break;
                }
            }

            Thread.sleep(100);
        }
    }

    public synchronized void trigger(Action action)
    {
        queue.addLast(action);
    }

    public synchronized void triggerStart()
    {
        queue.clear();
        trigger(Executor.Action.CONNECT);
        trigger(Executor.Action.VALIDATE);
        trigger(Executor.Action.START);
    }
    public synchronized void triggerStop()
    {
        queue.clear();
        trigger(Action.STOP);
    }

    private void connect() throws Exception
    {
        mySql.connect();
        logger.info("Connected to MySQL.");

        es.connect();
        logger.info("Connect to Elasticsearch.");
    }

    private void validate() throws Exception
    {
        mySql.validate();
    }

    private void start()
    {

        if (running.get())
            throw new RejectedExecutionException("Application is running.");

        running.set(true);

        Scheduler scheduler = Schedulers.from(threadPool);

        for (River.Database database: Setting.river.databases
                     ) {
            Emiter emiter = new Emiter(this, database);
            Consumer consumer = new Consumer(this, database);

            emiters.add(emiter);
            consumers.add(consumer);
            emiter.start();
            consumer.start();

            emiter.buildObservable(scheduler)

                //.subscribeOn(scheduler)
                .observeOn(scheduler)
                .subscribe(consumer);
        }
    }

    private void stop() {

        logger.trace("try to stop.");

        if (running.get())
        {
            running.set(false);

            logger.trace("stop all consumers.");
            consumers.forEach(Consumer::stop);

            logger.trace("stop all emiters.");
            emiters.forEach(Emiter::stop);
        }

        running.set(false);

        threadPool.shutdown();

        try {
            logger.trace("waiting threads stop.");

            while(!threadPool.awaitTermination(500, TimeUnit.MILLISECONDS))
            {
                logger.trace("LOOP Waiting Rx-threads stop.");
            }
        } catch (InterruptedException e)
        {

        }

        try {
            logger.trace("close mysql.");
            mySql.close();

            logger.trace("close elastic.");
            es.close();
        } catch (Exception e)
        {

        }

        logger.trace("all stoped.");
    }

    public synchronized void throwException(Throwable e)
    {
        Throwable throwable = e;
        while(throwable != null)
        {
            if (throwable instanceof InterruptedException || throwable instanceof UndeliverableException)
            {
                logger.warn("Get uncaught {} from {}, stop", e.getClass(), e.getCause().getClass());
                triggerStop();
                return;
            }

            throwable = throwable.getCause();
        }

        logger.error(e.getMessage(), e);

        if (e instanceof FatalException)
        {
            triggerStop();
        }

    }

    public Es getEs() {
        return es;
    }

    public MySql getMySql() {
        return mySql;
    }

    public Statistic getStatistic() {
        return statistic;
    }

    public ObjectMapper getJsonMapper() {

        return es.getJsonMapper();
    }

    public static boolean isRunning() {
        return running.get();
    }


}
