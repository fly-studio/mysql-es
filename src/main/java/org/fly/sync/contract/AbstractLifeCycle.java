package org.fly.sync.contract;

import org.fly.sync.exception.RunningException;

public class AbstractLifeCycle {
    protected volatile boolean running = false; // 是否处于运行中

    public boolean isStart() {
        return running;
    }

    public void start() {
        if (running) {
            throw new RunningException(this.getClass().getName() + " has startup , don't repeat start");
        }

        running = true;
    }

    public void stop() {
        if (!running) {
            throw new RunningException(this.getClass().getName() + " isn't start , please check");
        }

        running = false;
    }
}
