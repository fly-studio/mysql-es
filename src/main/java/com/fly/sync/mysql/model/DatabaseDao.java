package com.fly.sync.mysql.model;

import com.fly.core.database.SqlUtils;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterConstructorMapper(Database.class)
public interface DatabaseDao {

    @SqlQuery(SqlUtils.DATABASES + " WHERE SCHEMA_NAME = ?")
    Database find(String db);
}
