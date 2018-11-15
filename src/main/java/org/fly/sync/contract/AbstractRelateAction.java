package org.fly.sync.contract;

import org.fly.sync.setting.River;

public interface AbstractRelateAction extends AbstractRecordAction {

    River.Associate getAssociate();

}
