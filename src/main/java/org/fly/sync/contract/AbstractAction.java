package org.fly.sync.contract;

import java.io.Serializable;

public interface AbstractAction extends Serializable {

    void execute(AbstractWriter writer);
    String getGroup();
}
