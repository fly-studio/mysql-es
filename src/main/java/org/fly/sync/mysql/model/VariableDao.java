package org.fly.sync.mysql.model;

import org.fly.core.database.SqlUtils;
import org.fly.sync.contract.DaoInterface;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

@RegisterConstructorMapper(Variable.class)
public interface VariableDao extends DaoInterface {

    @SqlQuery(SqlUtils.VARIABLES + " LIKE ?")
    Variable find(String variableName);
}
