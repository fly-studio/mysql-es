package org.fly.sync.exception;

import java.sql.SQLException;

public class BinLogFormatException extends SQLException {
    public BinLogFormatException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public BinLogFormatException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public BinLogFormatException(String reason) {
        super(reason);
    }

    public BinLogFormatException() {
    }

    public BinLogFormatException(Throwable cause) {
        super(cause);
    }

    public BinLogFormatException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public BinLogFormatException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }

    public BinLogFormatException(String reason, String sqlState, int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
    }
}
