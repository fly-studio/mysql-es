package com.fly.sync.action;


import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import io.reactivex.annotations.Nullable;

public class NullAction implements AbstractAction {

    @Override
    public void execute(DbFactory dbFactory) {

    }

    @Override
    public String getGroup() {
        return Nullable.class.getName();
    }
}
