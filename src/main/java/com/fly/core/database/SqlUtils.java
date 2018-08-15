package com.fly.core.database;

public class SqlUtils {

    public final static String DATABASES = "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA ";
    public final static String DATABASE_NAMES = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ";
    public final static String TABLES = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? ";
    public final static String TABLE_NAMES = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? ";
    public final static String COLUMNS = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ";
    public final static String COLUMN_NAMES = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ";
    public static final String HEARTBEAT = "SELECT 1 ";
    public static final String VARIABLES = "Show VARIABLES ";
}
