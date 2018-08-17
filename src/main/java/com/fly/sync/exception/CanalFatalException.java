package com.fly.sync.exception;

public class CanalFatalException extends FatalException {
    public CanalFatalException(String message) {
        super(message);
    }

    public CanalFatalException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanalFatalException(Throwable cause) {
        super(cause);
    }

    public CanalFatalException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
