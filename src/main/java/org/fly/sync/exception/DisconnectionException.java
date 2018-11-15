package org.fly.sync.exception;

public class DisconnectionException extends Exception {
    public DisconnectionException(String message) {
        super(message);
    }

    public DisconnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public DisconnectionException(Throwable cause) {
        super(cause);
    }

    public DisconnectionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
