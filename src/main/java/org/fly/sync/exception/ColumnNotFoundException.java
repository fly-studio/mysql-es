package org.fly.sync.exception;

import java.sql.SQLException;

public class ColumnNotFoundException extends SQLException {
    public ColumnNotFoundException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public ColumnNotFoundException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public ColumnNotFoundException(String reason) {
        super(reason);
    }

    public ColumnNotFoundException(Throwable cause) {
        super(cause);
    }

    public ColumnNotFoundException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public ColumnNotFoundException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }

    public ColumnNotFoundException(String reason, String sqlState, int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
    }
}
