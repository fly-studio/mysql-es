package org.fly.sync.action;


import org.fly.sync.contract.AbstractAction;
import org.fly.sync.contract.AbstractWriter;
import io.reactivex.annotations.Nullable;

public class NullAction implements AbstractAction {

    public void execute(AbstractWriter writer) {

    }

    @Override
    public String getGroup() {
        return Nullable.class.getName();
    }
}
