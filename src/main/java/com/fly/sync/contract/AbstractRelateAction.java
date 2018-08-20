package com.fly.sync.contract;

import com.fly.sync.setting.River;

public interface AbstractRelateAction extends AbstractRecordAction {

    River.Associate getAssociate();

}
