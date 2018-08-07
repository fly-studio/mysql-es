package com.fly.sync.action;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.setting.BinLog;

public class ChangePostionAction extends BinLog.Position implements AbstractAction {

    public ChangePostionAction(String name, long position) {
        super(name, position);
    }

    public static ChangePostionAction create(BinLog.Position position) {
        return new ChangePostionAction(position.name, position.position);
    }

}
