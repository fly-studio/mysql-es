package org.fly.sync.contract;

import org.fly.sync.mysql.model.Record;
import org.elasticsearch.action.DocWriteRequest;

public interface AbstractRecordAction extends AbstractAction {

    Record getRecord();

    DocWriteRequest getRequest(DbFactory dbFactory) throws Exception;
}
