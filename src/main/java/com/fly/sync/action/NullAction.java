package com.fly.sync.action;


import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;

public class NullAction implements AbstractAction {

    @Override
    public void execute(DbFactory dbFactory) {

    }
}
