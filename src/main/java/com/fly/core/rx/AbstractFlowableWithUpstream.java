package com.fly.core.rx;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.fuseable.HasUpstreamPublisher;
import org.reactivestreams.Publisher;

/**
 * Abstract base class for operators that take an upstream
 * source {@link Publisher}.
 *
 * @param <T> the upstream items type
 * @param <R> the output items type
 */
abstract class AbstractFlowableWithUpstream<T, R> extends Flowable<R> implements HasUpstreamPublisher<T> {

    /**
     * The upstream source Publisher.
     */
    protected final Flowable<T> source;

    /**
     * Constructs a FlowableSource wrapping the given non-null (verified)
     * source Publisher.
     * @param source the source (upstream) Publisher instance, not null (verified)
     */
    AbstractFlowableWithUpstream(Flowable<T> source) {
        this.source = ObjectHelper.requireNonNull(source, "source is null");
    }

    @Override
    public final Publisher<T> source() {
        return source;
    }
}

