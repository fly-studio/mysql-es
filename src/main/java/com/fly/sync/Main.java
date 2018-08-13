package com.fly.sync;

import com.fly.sync.executor.Executor;
import com.fly.sync.setting.Setting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    public final static Logger logger = LoggerFactory.getLogger(Main.class);
    public final static String NAME = "Mysql_Es_Sync";
    public final static String DESCRIPTION = "Sync MySQL to ElasticSearch with relationship";
    public final static String VERSION = "1.0.0";

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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (Executor.isRunning())
                {
                    executor.close();
                    executor.stop();
                    logger.info("## stop the sync");
                }

            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal Server:", e);
            } finally {
                logger.info("## server is down.");
            }
        }));

        try {
            executor.connect();
            executor.validate();
            executor.run();
            executor.await();
        } catch (Exception e)
        {
            logger.error(e.getMessage(), e);
        } finally {
            executor.close();
            executor.stop();
        }

    }

    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e)  -> {
            logger.error("UnCaughtException", e);
        });
    }

}
