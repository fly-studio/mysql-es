package com.fly.sync.exception;

public class DumpFatalException extends FatalException {
    public DumpFatalException(String message) {
        super(message);
    }

    public DumpFatalException(String message, Throwable cause) {
        super(message, cause);
    }

    public DumpFatalException(Throwable cause) {
        super(cause);
    }

    public DumpFatalException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
