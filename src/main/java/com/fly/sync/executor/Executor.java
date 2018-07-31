package com.fly.sync.executor;

import com.fly.sync.Main;
import com.fly.sync.exception.EsException;
import com.fly.sync.exception.FatalException;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class Executor {

    private EsExecutor esExecutor;
    //private Map<River.Database, Thread> threads = new HashMap<>();
    private static boolean running = false;
    public final static Logger logger = LoggerFactory.getLogger(Main.class);
    private ExecutorService threadPool;

    public Executor()
    {
        esExecutor = new EsExecutor(Setting.river, true);
    }

    public void connect() throws Exception
    {
        if (!esExecutor.connect())
            throw new EsException("Can not connect to Elasticsearch.");
    }

    public void close()
    {
        esExecutor.close();
    }

    public void run()
    {
        if (running)
            throw new RejectedExecutionException("Application is running.");

        running = true;

        threadPool = Executors.newFixedThreadPool(Setting.river.databases.size());

        for (River.Database database: Setting.river.databases
                     ) {
            Thread thread = new ExecutorThread(this, database);

            //threads.put(database, thread);
            threadPool.submit(thread);
        }

        //threadPool.shutdown();

    }

    public void await() throws InterruptedException
    {
        while(!threadPool.awaitTermination(100, TimeUnit.MILLISECONDS));
    }


    public void throwException(Thread thread, Exception e)
    {
        synchronized (Executor.class)
        {
            logger.error(e.getMessage(), e);
            if (e instanceof FatalException)
            {
                threadPool.shutdownNow();
                /*for (Map.Entry<River.Database, Thread> entry: threads.entrySet()
                     ) {
                    entry.getValue().interrupt();
                }*/
            }
        }

    }
}
