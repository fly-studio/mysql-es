package org.fly.sync.mysql.model;

import org.fly.sync.contract.ModelInterface;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.beans.ConstructorProperties;


public class Database implements ModelInterface {

    public String CATALOG_NAME;
    public String SCHEMA_NAME;
    public String DEFAULT_CHARACTER_SET_NAME;
    public String DEFAULT_COLLATION_NAME;
    public String SQL_PATH;

    @JdbiConstructor
    @ConstructorProperties({"CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH"})
    public Database(String CATALOG_NAME,
                    String SCHEMA_NAME,
                    String DEFAULT_CHARACTER_SET_NAME,
                    String DEFAULT_COLLATION_NAME,
                    String SQL_PATH) {
        this.CATALOG_NAME = CATALOG_NAME;
        this.SCHEMA_NAME = SCHEMA_NAME;
        this.DEFAULT_CHARACTER_SET_NAME = DEFAULT_CHARACTER_SET_NAME;
        this.DEFAULT_COLLATION_NAME = DEFAULT_COLLATION_NAME;
        this.SQL_PATH = SQL_PATH;
    }
}
