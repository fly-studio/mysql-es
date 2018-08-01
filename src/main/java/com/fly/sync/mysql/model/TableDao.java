package com.fly.sync.mysql.model;

import com.fly.core.database.SqlUtils;
import com.fly.sync.contract.DaoInterface;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterConstructorMapper(Table.class)
public interface TableDao extends DaoInterface {

    @SqlQuery(SqlUtils.TABLES + " AND TABLE_NAME = ?")
    Table find(String db, String table);
}
