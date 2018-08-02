package com.fly.sync.exception;

public class FatalEsException extends FatalException{
    public FatalEsException(String message) {
        super(message);
    }

    public FatalEsException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalEsException(Throwable cause) {
        super(cause);
    }

    public FatalEsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
