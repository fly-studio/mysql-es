package com.fly.sync.exception;

public class RunningException extends FatalException {
    public RunningException(String message) {
        super(message);
    }

    public RunningException(String message, Throwable cause) {
        super(message, cause);
    }

    public RunningException(Throwable cause) {
        super(cause);
    }

    public RunningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
