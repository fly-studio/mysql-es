package org.fly.sync.exception;

public class EsException extends Exception {
    public EsException(String message) {
        super(message);
    }

    public EsException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsException(Throwable cause) {
        super(cause);
    }

    public EsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
