package com.fly.sync.executor;

import com.fly.sync.contract.DatabaseListener;
import com.fly.sync.exception.FatalDumpException;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;

public class On implements DatabaseListener {

    private Executor executor;
    private Thread thread;

    public On(Executor executor, Thread thread) {
        this.executor = executor;
        this.thread = thread;
    }



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
    public void onDelete(River.Database database, String sql) {

    }

    @Override
    public void onPositionChange(River.Database database, BinLog.Position position) {
        setBinLog(database, position);
    }

    @Override
    public void onError(River.Database database, Exception error) {
        if (error instanceof FatalDumpException)
            setBinLog(database, null);

        executor.throwException(thread, error);
    }

    private void setBinLog(River.Database database, BinLog.Position position)
    {
        if (position == null)
            Setting.binLog.remove(database.db);
        else
            Setting.binLog.set(database.db, position);

        try {
            Setting.saveBinLog();
        } catch (Exception e) {
            onError(database, e);
        }
    }
}
