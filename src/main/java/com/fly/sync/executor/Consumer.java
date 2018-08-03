package com.fly.sync.executor;

import com.fly.sync.contract.AbstractAction;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;

import java.util.List;

public class Consumer implements Observer<List<AbstractAction>> {
    @Override
    public void onSubscribe(Disposable disposable) {

    }

    @Override
    public void onNext(List<AbstractAction> abstractActions) {

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }
}
