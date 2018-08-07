package com.fly.sync.executor;

import com.fly.sync.contract.DatabaseListener;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.es.Es;
import com.fly.sync.exception.FatalDumpException;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;

public class On implements DatabaseListener {

    private DbFactory factory;

    public On(DbFactory factory) {
        this.factory = factory;
    }

    public River.Database getDatabase()
    {
        return factory.getRiverDatabase();
    }

    public Es getEs()
    {
        return factory.getEs();
    }

    public MySql getMySql()
    {
        return factory.getMySql();
    }

    @Override
    public void onCreateTable(String table) {

    }

    @Override
    public void onInsert(String sql) {

    }

    @Override
    public void onUpdate(String sql) {

    }

    @Override
    public void onDelete(String sql) {

    }

    @Override
    public void onPositionChange(BinLog.Position position) {
        setBinLog(position);
    }

    @Override
    public void onError(Exception error) {
        if (error instanceof FatalDumpException)
            setBinLog(null);

        //factory.throwException(error);
    }

    private void setBinLog(BinLog.Position position)
    {
        River.Database database = getDatabase();

        if (position == null)
            Setting.binLog.remove(database.schemaName);
        else
            Setting.binLog.set(database.schemaName, position);

        try {
            Setting.saveBinLog();
        } catch (Exception e) {
            onError(e);
        }
    }
}
