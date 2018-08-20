package com.fly.sync.contract;

public interface AbstractWriter {

    DbFactory getDbFactory();

    void updateByQuery(AbstractRelateAction relateAction);
}
