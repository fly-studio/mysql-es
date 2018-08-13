package com.fly.sync.contract;

import com.fly.sync.mysql.model.Record;
import org.elasticsearch.action.DocWriteRequest;

public interface AbstractRecordAction extends AbstractAction {

    Record getRecord();

    DocWriteRequest getRequest(DbFactory dbFactory) throws Exception;
}
