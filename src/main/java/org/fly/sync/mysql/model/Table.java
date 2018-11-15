package org.fly.sync.mysql.model;

import org.fly.sync.contract.ModelInterface;

import java.beans.ConstructorProperties;
import java.time.LocalDateTime;

public class Table implements ModelInterface {

    public String TABLE_CATALOG;
    public String TABLE_SCHEMA;
    public String TABLE_NAME;
    public String TABLE_TYPE;
    public String ENGINE;
    public long VERSION;
    public String ROW_FORMAT;
    public long TABLE_ROWS;
    public long AVG_ROW_LENGTH;
    public long DATA_LENGTH;
    public long MAX_DATA_LENGTH;
    public long INDEX_LENGTH;
    public long DATA_FREE;
    public long AUTO_INCREMENT;
    public LocalDateTime CREATE_TIME;
    public LocalDateTime UPDATE_TIME;
    public LocalDateTime CHECK_TIME;
    public String TABLE_COLLATION;
    public long CHECKSUM;
    public String CREATE_OPTIONS;
    public String TABLE_COMMENT;

    @ConstructorProperties({"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "TABLE_COLLATION", "CHECKSUM", "CREATE_OPTIONS", "TABLE_COMMENT"})
    public Table(String TABLE_CATALOG,
                 String TABLE_SCHEMA,
                 String TABLE_NAME,
                 String TABLE_TYPE,
                 String ENGINE,
                 long VERSION,
                 String ROW_FORMAT,
                 long TABLE_ROWS,
                 long AVG_ROW_LENGTH,
                 long DATA_LENGTH,
                 long MAX_DATA_LENGTH,
                 long INDEX_LENGTH,
                 long DATA_FREE,
                 long AUTO_INCREMENT,
                 LocalDateTime CREATE_TIME,
                 LocalDateTime UPDATE_TIME,
                 LocalDateTime CHECK_TIME,
                 String TABLE_COLLATION,
                 long CHECKSUM,
                 String CREATE_OPTIONS,
                 String TABLE_COMMENT) {
        this.TABLE_CATALOG = TABLE_CATALOG;
        this.TABLE_SCHEMA = TABLE_SCHEMA;
        this.TABLE_NAME = TABLE_NAME;
        this.TABLE_TYPE = TABLE_TYPE;
        this.ENGINE = ENGINE;
        this.VERSION = VERSION;
        this.ROW_FORMAT = ROW_FORMAT;
        this.TABLE_ROWS = TABLE_ROWS;
        this.AVG_ROW_LENGTH = AVG_ROW_LENGTH;
        this.DATA_LENGTH = DATA_LENGTH;
        this.MAX_DATA_LENGTH = MAX_DATA_LENGTH;
        this.INDEX_LENGTH = INDEX_LENGTH;
        this.DATA_FREE = DATA_FREE;
        this.AUTO_INCREMENT = AUTO_INCREMENT;
        this.CREATE_TIME = CREATE_TIME;
        this.UPDATE_TIME = UPDATE_TIME;
        this.CHECK_TIME = CHECK_TIME;
        this.TABLE_COLLATION = TABLE_COLLATION;
        this.CHECKSUM = CHECKSUM;
        this.CREATE_OPTIONS = CREATE_OPTIONS;
        this.TABLE_COMMENT = TABLE_COMMENT;
    }
}
