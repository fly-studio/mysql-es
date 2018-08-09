package com.fly.sync.mysql;

import com.fly.sync.action.RecordsAction;
import com.fly.sync.contract.AbstractConnector;
import com.fly.sync.exception.DisconnectionException;
import com.fly.sync.exception.RecordNotFoundException;
import com.fly.sync.mysql.model.DatabaseDao;
import com.fly.sync.mysql.model.Record;
import com.fly.sync.mysql.model.TableDao;
import com.fly.sync.setting.River;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.Driver;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.result.Field;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class MySql  {

    public final static Logger logger = LoggerFactory.getLogger(MySql.class);
    public Connector connector;
    private River river;
    private Map<String, List<String>> columnsCache = new HashMap<>();
    private Map<String, ColumnDefinition> columnDefinitionCache = new HashMap<>();

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

    public List<String> columnNames(String db, String table) throws SQLException
    {
        return columnNames(db, table, false);
    }

    public List<String> columnNames(ColumnDefinition columnDefinition)
    {
        List<String> columnNames = new ArrayList<>();
        Field[] fields = columnDefinition.getFields();
        for (int i = 0; i < fields.length; ++i)
            columnNames.add(fields[i].getOriginalName() != null ? fields[i].getOriginalName() : fields[i].getName());

        return columnNames;
    }

    public List<String> columnNames(String db, String table, boolean flush) throws SQLException
    {
        String key = db + "." + table;

        if (!flush)
        {
            if (columnsCache.containsKey(key))
                return columnsCache.get(key);
        }

        // return getClient().withExtension(ColumnDao.class, dao -> dao.allNames(db, table));

        List<String> columnNames = columnNames(columnDefinition(db, table, flush));
        columnsCache.put(key, columnNames);

        return columnNames;
    }

    public ColumnDefinition columnDefinition(String db, String table, boolean flush) throws SQLException
    {
        String key = "`" + db + "`.`" + table + "`";
        if (!flush)
        {
            if (columnDefinitionCache.containsKey(key))
                return columnDefinitionCache.get(key);
        }

        Resultset rs = (Resultset)getConnection().createStatement().executeQuery("SELECT * FROM "+ key + " LIMIT 0");

        ColumnDefinition cd = rs.getColumnDefinition();
        columnDefinitionCache.put(key, cd);

        return cd;
    }

    public ColumnDefinition columnDefinition(String db, String table) throws SQLException
    {
        return columnDefinition(db, table, false);
    }

    public ResultSetImpl buildResultSet(List<List<String>> data, ColumnDefinition columnDefinition) throws SQLException, ArrayIndexOutOfBoundsException
    {
        int columnCount = columnDefinition.getFields().length;

        List<ByteArrayRow> byteArrayRowList = new ArrayList<>();

        for (int i = 0; i < data.size(); ++i) {
            List<String> line = data.get(i);

            if (line.size() != columnCount)
                throw new ArrayIndexOutOfBoundsException("data["+ i +"] size != columnDefinition fields length");

            byte[][] rowBytes = new byte[columnCount][];

            for (int j = 0; j < columnCount; j++)
                rowBytes[j] = line.get(j) == null ? null : line.get(j).getBytes();

            byteArrayRowList.add(new ByteArrayRow(rowBytes, null));
        }

        ResultsetRowsStatic newRsRow = new ResultsetRowsStatic(byteArrayRowList, columnDefinition);

        return new ResultSetFactory(getConnection(), null)
                .createFromResultsetRows(java.sql.ResultSet.CONCUR_READ_ONLY, java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, newRsRow);
    }

    public ResultSetImpl buildResultSet(List<List<String>> data, String db, String table) throws SQLException, ArrayIndexOutOfBoundsException
    {
        return buildResultSet(data, columnDefinition(db, table));
    }

    public Record mixRecord(String db, String table, List<String> data)
    {
        RecordsAction recordsAction = mixRecords(db, table, Arrays.asList(data));

        return recordsAction == null || recordsAction.isEmpty() ? null : recordsAction.get(0);
    }

    public RecordsAction mixRecords(String db, String table, List<List<String>> data)
    {
        try
        {
            ResultSetImpl resultSet = buildResultSet(data, db, table);
            List<String> columnNames = columnNames(db, table);

            RecordsAction recordsAction = new RecordsAction();

            while (resultSet.next())
            {
                Map<String, Object> row = new HashMap<>();

                for(int i = 1; i <= columnNames.size(); ++i)
                    row.put(columnNames.get(i - 1), resultSet.getObject(i));

                recordsAction.add(Record.create(table, row));
            }

            return recordsAction;

        } catch (SQLException|ArrayIndexOutOfBoundsException e)
        {
            logger.error("Mix recordsAction failed.", e);
            return null;
        }
    }

    public RecordsAction queryIn(River.TableBase table, String whereInColumn, List<String> values)
    {
        return getClient().withHandle(handle -> {

            String sql = table.toSql(whereInColumn);
            RecordsAction recordsAction = new RecordsAction();

            List<Map<String, Object>> list = handle.createQuery(sql)
                    .bindList("Values", values)
                    .mapToMap()
                    .list()
                    .stream()
                    .filter(v -> v != null)
                    .collect(Collectors.toList());

            list.forEach(kv -> recordsAction.add(Record.create(table.tableName, kv)));

            return recordsAction;
        });
    }

    public void validate() throws SQLException
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
                table.validateColumns(columnNames(database.schemaName, table.tableName));

                for (Map.Entry<String, River.Relation> relationEntry: tableEntry.getValue().relations.entrySet()
                     ) {
                    River.Relation relation = relationEntry.getValue();
                    relation.validateColumns(columnNames(database.schemaName, relation.tableName));
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
                .append("?useSSL=false&verifyServerCertificate=false&useUnicode=true&characterEncoding=")
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
