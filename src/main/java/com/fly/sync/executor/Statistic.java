package com.fly.sync.executor;

import java.util.concurrent.atomic.AtomicLong;

public class Statistic {
    private AtomicLong subscribeCount = new AtomicLong(0);
    private AtomicLong recordCount = new AtomicLong(0);
    private long createdAt = System.currentTimeMillis();

    public Statistic() {
    }


    public AtomicLong getSubscribeCount() {
        return subscribeCount;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public AtomicLong getRecordCount() {
        return recordCount;
    }
}
