package com.fly.sync.mysql.model;

import com.fly.sync.contract.ModelInterface;

public class Column implements ModelInterface {

    public String TABLE_CATALOG;
    public String TABLE_SCHEMA;
    public String TABLE_NAME;
    public String COLUMN_NAME;
    public long ORDINAL_POSITION;
    public String COLUMN_DEFAULT;
    public String IS_NULLABLE;
    public String DATA_TYPE;
    public long CHARACTER_MAXIMUM_LENGTH;
    public long CHARACTER_OCTET_LENGTH;
    public long NUMERIC_PRECISION;
    public long NUMERIC_SCALE;
    public long DATETIME_PRECISION;
    public String CHARACTER_SET_NAME;
    public String COLLATION_NAME;
    public String COLUMN_TYPE;
    public String COLUMN_KEY;
    public String EXTRA;
    public String PRIVILEGES;
    public String COLUMN_COMMENT;
    public String GENERATION_EXPRESSION;

}
