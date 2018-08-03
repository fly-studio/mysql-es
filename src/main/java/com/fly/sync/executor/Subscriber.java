package com.fly.sync.executor;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.setting.River;
import org.reactivestreams.Subscription;

import java.util.List;

public class Subscriber implements org.reactivestreams.Subscriber<List<AbstractAction>>
{
    private Executor executor;
    private River.Database database;
    private Subscription subscription;

    public Subscriber(Executor executor, River.Database database) {
        this.executor = executor;
        this.database = database;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    private int i = 0, n = 0;
    @Override
    public void onNext(List<AbstractAction> actionList) {

        // TODO
        n ++;
        i = actionList.size();
        System.out.println(i + ":" + n);

        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        executor.throwException(throwable);
    }

    @Override
    public void onComplete() {
        System.out.println("Complete.");
        executor.stop();
    }
}