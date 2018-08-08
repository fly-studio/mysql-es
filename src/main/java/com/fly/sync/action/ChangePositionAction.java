package com.fly.sync.action;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.Setting;

public class ChangePositionAction extends BinLog.Position implements AbstractAction {

    public ChangePositionAction(String name, long position) {
        super(name, position);
    }

    public static ChangePositionAction create(BinLog.Position position) {
        return new ChangePositionAction(position.name, position.position);
    }

    @Override
    public void execute(DbFactory dbFactory) {
        try
        {
            Setting.binLog.set(dbFactory.getRiverDatabase().schemaName, this);
            Setting.saveBinLog();
        } catch (Exception e)
        {
            logger.error("Save BinLog Position Failed.", e);
        }
    }
}
