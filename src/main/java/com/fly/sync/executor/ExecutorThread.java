package com.fly.sync.executor;

import com.fly.sync.contract.DatabaseListener;
import com.fly.sync.mysql.Dumper;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;

public class ExecutorThread extends Thread {

    private River.Database database;
    private Executor executor;
    private DatabaseListener listener;


    public ExecutorThread(Executor executor, River.Database database) {
        super();

        this.executor = executor;
        this.database = database;
        listener = new On(executor, this);
    }

    @Override
    public void run() {

        runDumper(database);

        while (!isInterrupted()){ //非阻塞过程中通过判断中断标志来退出
            try{

                if (runCanal(database) <= 0)
                    Thread.sleep(Setting.config.flushBulkTime);

            } catch (InterruptedException e){

                break;//捕获到异常之后，执行break跳出循环。
            }
        }
    }

    public void runDumper(River.Database database)
    {
        Dumper dumper = new Dumper(Setting.config, Setting.river, database, listener);

        if (Setting.binLog.get(database.db) != null) {
            return;
        }
        dumper.run();

    }

    public int runCanal(River.Database database)
    {
        System.out.println("run Cannal.");
        return 0;
    }


}
