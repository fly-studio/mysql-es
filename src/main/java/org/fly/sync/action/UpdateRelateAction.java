package org.fly.sync.action;

import org.fly.sync.contract.AbstractRelateAction;
import org.fly.sync.contract.AbstractWriter;
import org.fly.sync.contract.DbFactory;
import org.fly.sync.mysql.model.Record;
import org.fly.sync.setting.River;
import org.elasticsearch.action.DocWriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateRelateAction implements AbstractRelateAction {
    public final static Logger logger = LoggerFactory.getLogger(UpdateRelateAction.class);

    private Record record;
    private River.Associate associate;

    public UpdateRelateAction(Record record, River.Associate associate) {
        this.record = record;
        this.associate = associate;
    }

    public static UpdateRelateAction create(Record record, River.Associate associate) {
        return new UpdateRelateAction(record, associate);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public DocWriteRequest getRequest(DbFactory dbFactory) throws Exception {
        return null;
    }

    @Override
    public void execute(AbstractWriter writer) {
        writer.updateByQuery(this);
    }

    @Override
    public String getGroup() {
        return AbstractRelateAction.class.getName();
    }

    @Override
    public River.Associate getAssociate() {
        return associate;
    }

}
