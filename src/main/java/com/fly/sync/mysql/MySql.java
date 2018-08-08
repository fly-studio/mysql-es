package com.fly.sync.mysql;

import com.fly.sync.contract.AbstractConnector;
import com.fly.sync.exception.ColumnNotFoundException;
import com.fly.sync.exception.DisconnectionException;
import com.fly.sync.exception.RecordNotFoundException;
import com.fly.sync.mysql.model.*;
import com.fly.sync.setting.River;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.Driver;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MySql  {

    public Connector connector;
    private River river;
    private Map<String, List<String>> columnsCache = new HashMap<>();
    private Map<String, Resultset> blankResultsets = new HashMap<>();

    public MySql(River river, boolean autoReconnect) {
        this.river = river;
        connector = new Connector(river, autoReconnect);

    }

    public boolean connect() throws Exception
    {
        return this.connector.connect();
    }

    public void close() throws Exception
    {
        this.connector.close();
    }

    public Jdbi getClient()
    {
        return connector.getClient();
    }

    public ConnectionImpl getConnection()
    {
        return connector.getConnection();
    }

    public boolean exists(String db)
    {
        return getClient().withExtension(DatabaseDao.class, dao -> dao.find(db) != null);
    }

    public boolean exists(String db, String table)
    {
        return getClient().withExtension(TableDao.class, dao -> dao.find(db, table) != null);
    }

    public List<String> columns(String db, String table)
    {
        return columns(db, table, false);
    }

    public List<String> columns(String db, String table, boolean flush)
    {
        String key = db + "." + table;
        if (!flush)
        {
            if (columnsCache.containsKey(key))
                return columnsCache.get(key);
        }

        List<String> list = getClient().withExtension(ColumnDao.class, dao -> dao.allNames(db, table));
        columnsCache.put(key, list);
        return list;
    }

    public Resultset getBlankResultset(String db, String table) throws SQLException
    {
        return getBlankResultset(db, table, false);
    }

    public Resultset getBlankResultset(String db, String table, boolean flush) throws SQLException
    {
        String key = "`" + db + "`.`" + table + "`";
        if (!flush)
        {
            if (blankResultsets.containsKey(key))
                return blankResultsets.get(key);
        }
        Resultset rs = (Resultset)getConnection().createStatement().executeQuery("SELECT * FROM "+ key + " LIMIT 1");

        blankResultsets.put(key, rs);
        return rs;
    }

    public Record mixResultset(String db, String table, List<String> data) throws SQLException
    {
        Resultset rs = getBlankResultset(db, table);
        ColumnDefinition columnDefinition = rs.getColumnDefinition();

        if (columnDefinition.getFields().length != data.size())
            return null;

        if (rs instanceof ResultSetImpl)
        {
            ResultSetImpl resultSet = (ResultSetImpl) rs;
            ResultsetRowsStatic rsRow = (ResultsetRowsStatic)resultSet.getRows();

            byte[][] rowBytes = new byte[columnDefinition.getFields().length][];

            for (int i = 0; i < columnDefinition.getFields().length; i++)
                rowBytes[i] = data.get(i) == null ? null : data.get(i).getBytes();

            ResultsetRowsStatic newRsRow = new ResultsetRowsStatic(Arrays.asList(new ByteArrayRow(rowBytes, null)), rsRow.getMetadata());
            ResultSetImpl resultSetImp = new ResultSetFactory(getConnection(), (StatementImpl) resultSet.getStatement())
                    .createFromResultsetRows(resultSet.getConcurrency(), resultSet.getType(), newRsRow);

            if (resultSetImp.next())
            {
                ResultSetMetaData m = resultSetImp.getMetaData();
                int columnCount = m.getColumnCount();
                String[] columnNames = new String[columnCount + 1];

                for(int i = 1; i <= columnCount; ++i) {
                    String key = m.getColumnName(i);
                    String alias = m.getColumnLabel(i);
                    if (alias == null) {
                        alias = key;
                    }

                    columnNames[i] = alias;
                }

                Map<String, Object> row = new HashMap<>();
                for(int i = 1; i <= columnCount; ++i) {
                    row.put(columnNames[i], resultSetImp.getObject(i));
                }
                return Record.create(table, row);
            }

            return null;
        }

        return null;
    }

    public Records query(River.TableBase table, String whereInColumn, List<String> values)
    {
        return getClient().withHandle(handle -> {

            String sql = table.toSql(whereInColumn);
            Records records = new Records();

            List<Map<String, Object>> list = handle.createQuery(sql)
                    .bindList("Values", values)
                    .mapToMap()
                    .list()
                    .stream()
                    .filter(v -> v != null)
                    .collect(Collectors.toList());

            list.forEach(kv -> records.add(Record.create(table.tableName, kv)));

            return records;
        });
    }

    public void validate() throws RecordNotFoundException, ColumnNotFoundException
    {
        for (River.Database database:
             river.databases) {
            if (!exists(database.schemaName))
                throw new RecordNotFoundException("Database \"" + database.schemaName + "\" is not exists");

            for (String tableName: database.associates.keySet()
                 ) {
                if (!exists(database.schemaName, tableName))
                    throw new RecordNotFoundException("Table \"" + database.schemaName + "." + tableName + "\" is not exists");
            }

            for (Map.Entry<String, River.Table> tableEntry: database.tables.entrySet()
                 ) {
                River.Table table = tableEntry.getValue();
                table.setFullColumns(columns(database.schemaName, table.tableName));

                for (Map.Entry<String, River.Relation> relationEntry: tableEntry.getValue().relations.entrySet()
                     ) {
                    River.Relation relation = relationEntry.getValue();
                    relation.setFullColumns(columns(database.schemaName, relation.tableName));
                }
            }
        }
    }

    public class Connector extends AbstractConnector
    {
        private Jdbi jdbi;
        private ConnectionImpl connection;
        public Connector(River river, boolean autoReconnect) {
            super(river, autoReconnect);
        }

        @Override
        protected void doConnecting() throws Exception
        {
            if (null != jdbi)
                return;
            StringBuilder sb = new StringBuilder();
            sb.append("jdbc:mysql://")
                .append(river.my.host)
                .append(":")
                .append(river.my.port)
                .append("/")
                .append("INFORMATION_SCHEMA")
                .append("?useUnicode=true&characterEncoding=")
                .append(river.charset);
            if (autoReconnect)
                sb.append("&autoReconnect=true&failOverReadOnly=true");

            new Driver();
            jdbi = Jdbi.create(sb.toString(), river.my.user, river.my.password);
            jdbi.installPlugin(new SqlObjectPlugin());
            connection = (ConnectionImpl)jdbi.open().getConnection();
        }

        @Override
        protected void doReconnect() throws Exception {
            jdbi = null;
            doConnecting();
        }

        @Override
        protected void doHeartbeat() throws Exception
        {
            Integer i = jdbi.withHandle(handle ->
                 handle.createQuery("SELECT 1").mapTo(Integer.class).findOnly()
            );

            if (i == null || !i.equals(1))
                throw new DisconnectionException("MySQL: Heart beat Fails.");
        }

        @Override
        protected void doClose() throws Exception {
            connection.close();
        }


        public Jdbi getClient() {
            return jdbi;
        }

        public ConnectionImpl getConnection() {
            return connection;
        }
    }
}
