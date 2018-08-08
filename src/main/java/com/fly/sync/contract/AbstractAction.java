package com.fly.sync.contract;

import com.fly.sync.es.Es;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public interface AbstractAction extends Serializable {
    public final static Logger logger = LoggerFactory.getLogger(Es.class);

    void execute(DbFactory dbFactory);
}
