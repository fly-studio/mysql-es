package com.fly.sync;

import com.fly.sync.executor.Executor;
import com.fly.sync.setting.Setting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

public class Main {

    public final static Logger logger = LoggerFactory.getLogger(Main.class);
    public final static String NAME = "Mysql_Es_Sync";
    public final static String DESCRIPTION = "Sync MySQL to ElasticSearch with relationship";
    public final static String VERSION = "1.0.0";

    public final static Thread mainThread = Thread.currentThread();
    private static volatile boolean signalExit = false;


    public static void main(String[] argv) {

        try {
            Setting.readSettings();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return;
        }

        logger.info("## set default uncaught exception handler");
        setGlobalUncaughtExceptionHandler();

        Executor executor = new Executor();


        Signal.handle(new Signal("INT"), signal -> {
            logger.trace("Shutdown from INT signal: {}-{}", Thread.currentThread().getName(), Thread.currentThread().getId());

            signalExit = true;

            shutdown(executor);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.trace("Shutdown from Runtime hook.");

            if (!signalExit)
                shutdown(executor);
        }, "Shutdown-Thread"));

        //RxJavaPlugins.setErrorHandler(executor::throwException);

        try {

            executor.triggerStart();

            executor.run();

        } catch (InterruptedException e)
        {

        } catch (Exception e)
        {
            logger.error(e.getMessage(), e);
        }

        logger.info("Server down.");


    }

    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e)  -> {
            logger.error("UnCaughtException", e);
        });
    }

    private static void shutdown(Executor executor)
    {
         try {

            executor.triggerStop();

        } catch (Throwable e) {
            logger.error("##something goes wrong when stopping canal Server:", e);
        }
    }

}
