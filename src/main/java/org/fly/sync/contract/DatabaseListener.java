package org.fly.sync.contract;

import org.fly.sync.setting.BinLog;

public interface DatabaseListener {
    void onCreateTable(String table);
    void onInsert(String sql);
    void onUpdate(String sql);
    void onDelete(String sql);
    void onPositionChange(BinLog.Position position);
    void onError(Exception error);
}
