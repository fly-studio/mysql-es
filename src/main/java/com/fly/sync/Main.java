package com.fly.sync;

import com.fly.sync.executor.Executor;
import com.fly.sync.setting.Setting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    public final static Logger logger = LoggerFactory.getLogger(Main.class);
    private final static Thread mainThread = Thread.currentThread();

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
            logger.trace("Shutdown from Runtime hook.");

            if( LogManager.getContext() instanceof LoggerContext ) {
                logger.debug("Shutting down log4j2");
                Configurator.shutdown((LoggerContext)LogManager.getContext());
            } else
                logger.warn("Unable to shutdown log4j2");

            executor.stop();

            while(mainThread.getState() != Thread.State.TERMINATED) {
                try {
                    Thread.sleep(100);

                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                    return;
                }
            }

            System.out.println("Exit success.");
        }, "Shutdown-Thread"));

        //RxJavaPlugins.setErrorHandler(executor::throwException);

        try {

            executor.start();

            executor.run();

        } catch (InterruptedException e)
        {

        } catch (Exception e)
        {
            executor.stop();
            logger.error(e.getMessage(), e);
        }

        logger.info("Server down.");
    }

    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e)  -> {
            logger.error("UnCaughtException", e);
        });
    }

}
