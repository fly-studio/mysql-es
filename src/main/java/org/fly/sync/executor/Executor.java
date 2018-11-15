package org.fly.sync.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.fly.sync.Main;
import org.fly.sync.es.Es;
import org.fly.sync.mysql.MySql;
import org.fly.sync.setting.River;
import org.fly.sync.setting.Setting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
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

    public final static Logger logger = LoggerFactory.getLogger(Main.class);

    public enum Action {
        CONNECT,
        VALIDATE,
        LAUNCH,
        STOP,
        EXIT,
    }
    private Es es;
    private MySql mySql;
    private static AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService threadPool;
    private Statistic statistic = new Statistic();
    private List<Emiter> emiters = new ArrayList<>();
    private List<Consumer> consumers = new ArrayList<>();
    private Thread currentThread = null;
    private LinkedList<Action> queue = new LinkedList<>();

    public Executor()
    {
        threadPool = Executors.newFixedThreadPool(Setting.river.databases.size() * 10, new ThreadFactoryBuilder().setNameFormat("SubscribeOn-%d").build());
        es = new Es(Setting.river, true);
        mySql = new MySql(Setting.river, true);
    }

    public void run() throws Exception
    {
        currentThread = Thread.currentThread();

        while(true)
        {
            Action action = queue.isEmpty() ? null : queue.pop();

            if (action != null)
            {
                try {
                    switch (action)
                    {
                        case EXIT:
                            return;
                        case LAUNCH:
                            doLaunch();
                            break;
                        case STOP:
                            // stop 不应该抛出错误
                            doStop();
                            break;
                        case CONNECT:
                            doConnect();
                            break;
                        case VALIDATE:
                            doValidate();
                            break;
                    }
                } catch (Exception e)
                {
                    // 此处是在launch之前的错误
                    // 因为之后的错误都被RxJava统一捕获，并发送给Consumer
                    throwException(e);
                }
            }

            Thread.sleep(100);
        }
    }

    private synchronized void trigger(Action action)
    {
        queue.addLast(action);
    }

    public synchronized void start()
    {
        queue.clear();
        trigger(Executor.Action.CONNECT);
        trigger(Executor.Action.VALIDATE);
        trigger(Executor.Action.LAUNCH);
    }

    /**
     * 当run被调取之后，本线程（比如Main中调取就是主线程）会被阻塞直到插入EXIT到队列，
     * 如果在相同进程（比如Main中）调取stop，会发现根本无法响应，因为线程已经被阻塞根本无法执行后续代码。
     * 所以需要另起的线程调取stop，比如：Main中使用的ShutdownHook的线程调取, Observeable中使用Consumer(也在其他线程)
     */
    public synchronized void stop()
    {
        queue.clear();
        trigger(Action.STOP);
        trigger(Action.EXIT);
    }

    private void doConnect() throws Exception
    {
        mySql.connect();
        logger.info("Connected to MySQL.");

        es.connect();
        logger.info("Connect to Elastic search.");
    }

    private void doValidate() throws Exception
    {
        mySql.validate();
    }

    private void doLaunch()
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

    private void doStop() {

        boolean isLog4jEnable = false;

        if( LogManager.getContext() instanceof LoggerContext)
            isLog4jEnable = ((LoggerContext)LogManager.getContext()).isStarted();

        if (!isLog4jEnable)
            System.out.println("try to stop.");
        else
            logger.trace("try to stop.");

        threadPool.shutdown();

        if (running.get())
        {
            running.set(false);

            if (!isLog4jEnable)
                System.out.println("stop all consumers.");
            else
                logger.trace("stop all consumers.");
            consumers.forEach(Consumer::stop);

            if (!isLog4jEnable)
                System.out.println("stop all emiters.");
            else
                logger.trace("stop all emiters.");
            emiters.forEach(Emiter::stop);
        }
        
        running.set(false);

        try {
            if (!isLog4jEnable)
                System.out.println("waiting threads stop.");
            else
                logger.trace("waiting threads stop.");

            while(!threadPool.awaitTermination(500, TimeUnit.MILLISECONDS))
            {
                if (!isLog4jEnable)
                    System.out.println("LOOP Waiting Rx-threads stop.");
                else
                    logger.trace("LOOP Waiting Rx-threads stop.");
            }
        } catch (InterruptedException e)
        {

        }

        try {
            if (!isLog4jEnable)
                System.out.println("close mysql.");
            else
                logger.trace("close mysql.");

            mySql.close();

            if (!isLog4jEnable)
                System.out.println("close elastic.");
            else
                logger.trace("close elastic.");

            es.close();
        } catch (Exception e)
        {

        }

        if (!isLog4jEnable)
            System.out.println("all stoped.");
        else
            logger.trace("all stoped.");
    }

    public synchronized void throwException(Throwable e)
    {
        /*Throwable throwable = e;
        while(throwable != null)
        {
            if (throwable instanceof InterruptedException || throwable instanceof UndeliverableException)
            {
                logger.warn("Get uncaught {} from {}, stop", e.getClass(), e.getCause().getClass());
                stop();
                return;
            }

            throwable = throwable.getCause();
        }

        logger.error(e.getMessage(), e);

        if (e instanceof FatalException)
        {
            stop();
        }*/

        logger.error(e.getMessage(), e);
        stop();
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
