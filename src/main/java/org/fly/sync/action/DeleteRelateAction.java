package org.fly.sync.action;

import org.fly.sync.contract.AbstractRelateAction;
import org.fly.sync.contract.AbstractWriter;
import org.fly.sync.contract.DbFactory;
import org.fly.sync.mysql.model.Record;
import org.fly.sync.setting.River;
import org.elasticsearch.action.DocWriteRequest;

public class DeleteRelateAction implements AbstractRelateAction {

    private Record record;
    private River.Associate associate;

    public DeleteRelateAction(Record record, River.Associate associate) {
        this.record = record;
        this.associate = associate;
    }

    public static DeleteRelateAction create(Record record, River.Associate associate) {
        return new DeleteRelateAction(record, associate);
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
