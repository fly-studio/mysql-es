package com.fly.sync.exception;

public class FatalDumpException extends FatalException {
    public FatalDumpException(String message) {
        super(message);
    }

    public FatalDumpException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalDumpException(Throwable cause) {
        super(cause);
    }

    public FatalDumpException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
