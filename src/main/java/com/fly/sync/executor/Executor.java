package com.fly.sync.executor;

import com.fly.sync.contract.DatabaseListener;
import com.fly.sync.dumper.Dumper;
import com.fly.sync.exception.DumpException;
import com.fly.sync.exception.EsException;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;

import java.util.Set;

public class Executor {

    private EsExecutor esExecutor;

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

    public void run() throws Exception
    {
        for (River.Database database: Setting.river.databases
             ) {
            runDumper();
        }
    }

    private DatabaseListener listener = new DatabaseListener() {
        @Override
        public void onCreateTable(River.Database database, String table) {

        }

        @Override
        public void onInsert(River.Database database, String sql) {

        }

        @Override
        public void onUpdate(River.Database database, String sql) {

        }

        @Override
        public void onPostionChange(River.Database database, BinLog.Position position) {
            Setting.binLog.set(database.db, position);
            try {
                Setting.saveBinLog();
            } catch (Exception e) {
                this.onError(database, e);
            }
        }

        @Override
        public void onError(River.Database database, String error) {
            throwException(new DumpException(error));
        }

        @Override
        public void onError(River.Database database, Exception error) {
            throwException(new DumpException(error));
        }
    };

    private void runDumper() throws Exception
    {
        Dumper dumper = new Dumper(Setting.config, Setting.river);
        for (River.Database database: Setting.river.databases
             ) {

            if (Setting.binLog.get(database.db) != null)
                continue;

            dumper.run(database, listener);
        }
    }

    private void throwException(Exception e)
    {

    }
}
