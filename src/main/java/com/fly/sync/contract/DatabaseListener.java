package com.fly.sync.contract;

import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.River;

public interface DatabaseListener {
    void onCreateTable(River.Database database, String table);
    void onInsert(River.Database database, String sql);
    void onUpdate(River.Database database, String sql);
    void onDelete(River.Database database, String sql);
    void onPositionChange(River.Database database, BinLog.Position position);
    void onError(River.Database database, Exception error);
}
