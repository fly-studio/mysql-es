package org.fly.sync.exception;

public class DumpException extends Exception {

    public DumpException(String message) {
        super(message);
    }

    public DumpException(String message, Throwable cause) {
        super(message, cause);
    }

    public DumpException(Throwable cause) {
        super(cause);
    }

    public DumpException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
