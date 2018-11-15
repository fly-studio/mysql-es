package org.fly.sync.mysql.model;

import org.fly.core.database.SqlUtils;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import java.util.List;

@RegisterConstructorMapper(Column.class)
public interface ColumnDao {

    @SqlQuery(SqlUtils.COLUMNS)
    List<Column> all(String dbName, String tableName);

    @SqlQuery(SqlUtils.COLUMNS + " AND COLUMN_NAME IN (<ColumnNames>)")
    List<Column> findMany(@BindList("ColumnNames") List<String> columnNames);

    @SqlQuery(SqlUtils.COLUMN_NAMES)
    List<String> allNames(String dbName, String tableName);

}
