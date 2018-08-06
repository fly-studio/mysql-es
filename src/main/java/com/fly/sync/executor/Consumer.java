package com.fly.sync.executor;

import com.fly.sync.action.NullAction;
import com.fly.sync.contract.AbstractAction;
import com.fly.sync.setting.River;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;

import java.util.List;

public class Consumer implements Observer<List<AbstractAction>> {
    private Executor executor;
    private River.Database database;

    public Consumer(Executor executor, River.Database database) {
        this.executor = executor;
        this.database = database;
    }

    @Override
    public void onSubscribe(Disposable disposable) {

    }

    private int i = 0, n = 0;
    @Override
    public void onNext(List<AbstractAction> actionList) {
        n ++;
        i += actionList.size();

        for (AbstractAction action : actionList
        ) {
            if (action instanceof NullAction)
                System.out.println("canal run");

        }

        System.out.println(i + ":" + n);
        try {

            Thread.sleep(50);
        } catch (InterruptedException e) {}
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {
        executor.stop();
    }
}
