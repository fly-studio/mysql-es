package com.fly.sync.exception;

public class FatalCanalException extends FatalException {
    public FatalCanalException(String message) {
        super(message);
    }

    public FatalCanalException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalCanalException(Throwable cause) {
        super(cause);
    }

    public FatalCanalException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
