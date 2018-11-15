package org.fly.sync.exception;

public class EsFatalException extends FatalException{
    public EsFatalException(String message) {
        super(message);
    }

    public EsFatalException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsFatalException(Throwable cause) {
        super(cause);
    }

    public EsFatalException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
