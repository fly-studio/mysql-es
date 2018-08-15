package com.fly.sync.mysql.model;

import com.fly.core.database.SqlUtils;
import com.fly.sync.contract.DaoInterface;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterConstructorMapper(Variable.class)
public interface VariableDao extends DaoInterface {

    @SqlQuery(SqlUtils.VARIABLES + " LIKE ?")
    Variable find(String variableName);
}
