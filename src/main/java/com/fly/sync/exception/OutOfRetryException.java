package com.fly.sync.exception;

public class OutOfRetryException extends FatalException {
    public OutOfRetryException(String message) {
        super(message);
    }

    public OutOfRetryException(String message, Throwable cause) {
        super(message, cause);
    }

    public OutOfRetryException(Throwable cause) {
        super(cause);
    }

    public OutOfRetryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
