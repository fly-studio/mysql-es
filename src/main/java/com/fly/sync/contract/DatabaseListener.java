package com.fly.sync.contract;

import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.River;

public interface DatabaseListener {
    void onCreateTable(String table);
    void onInsert(String sql);
    void onUpdate(String sql);
    void onDelete(String sql);
    void onPositionChange(BinLog.Position position);
    void onError(Exception error);
}
