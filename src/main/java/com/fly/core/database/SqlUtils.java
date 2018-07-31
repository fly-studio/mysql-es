package com.fly.core.database;

public class SqlUtils {

    public final static String TABLES = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '?'";
    public final static String COLUMNS = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '?' AND TABLE_NAME = '?'";


}
