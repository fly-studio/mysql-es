package com.fly.sync.contract;

import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.River;

public interface DatabaseListener {
    void onCreateTable(River.Database database, String table);
    void onInsert(River.Database database, String sql);
    void onUpdate(River.Database database, String sql);
    void onPostionChange(River.Database database, BinLog.Position position);
    void onError(River.Database database, String error);
    void onError(River.Database database, Exception error);
}
