package com.fly.sync.mysql;

import com.fly.core.database.SqlUtils;
import com.fly.sync.contract.AbstractConnector;
import com.fly.sync.exception.BinLogFormatException;
import com.fly.sync.exception.DisconnectionException;
import com.fly.sync.exception.RecordNotFoundException;
import com.fly.sync.mysql.model.*;
import com.fly.sync.setting.River;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.Driver;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class MySql  {

    public final static Logger logger = LoggerFactory.getLogger(MySql.class);
    public Connector connector;
    private River river;
    private Map<String, List<String>> columnsCache = new HashMap<>();
    private Map<String, ColumnDefinition> columnDefinitionCache = new HashMap<>();
    private Query localQuery, utcQuery;

    public MySql(River river, boolean autoReconnect) {
        this.river = river;
        connector = new Connector(river, autoReconnect);
    }

    public boolean connect() throws Exception
    {
        boolean result = this.connector.connect();
        if (result)
        {
            localQuery = new Query(connector.getConnection());
            utcQuery = new Query(connector.getUtcConnection());
        }

        return result;
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

    public Query getLocalQuery() {
        return localQuery;
    }

    public Query getUtcQuery() {
        return utcQuery;
    }

    public void validate() throws SQLException
    {
        if (!localQuery.variable("binlog_format").equalsIgnoreCase("ROW") || !localQuery.variable("binlog_row_image").equalsIgnoreCase("FULL"))
            throw new BinLogFormatException("MySQL must enable the binlog, And binlog_row_image = FULL, binlog_format = ROW.");

        for (River.Database database:
             river.databases) {
            if (!localQuery.exists(database.schemaName))
                throw new RecordNotFoundException("Database \"" + database.schemaName + "\" is not exists");

            for (String tableName: database.associates.keySet()
                 ) {
                if (!localQuery.exists(database.schemaName, tableName))
                    throw new RecordNotFoundException("Table \"" + database.schemaName + "." + tableName + "\" is not exists");
            }

            for (Map.Entry<String, River.Table> tableEntry: database.tables.entrySet()
                 ) {

                River.Table table = tableEntry.getValue();
                table.validateColumns(localQuery.columnNames(database.schemaName, table.tableName));

                for (Map.Entry<String, River.Relation> relationEntry: tableEntry.getValue().relations.entrySet()
                     ) {
                    River.Relation relation = relationEntry.getValue();
                    relation.validateColumns(localQuery.columnNames(database.schemaName, relation.tableName));
                }
            }
        }
    }

    public class Query {

        ConnectionImpl connection;

        public Query(ConnectionImpl connection) {
            this.connection = connection;
        }

        public boolean exists(String db)
        {
            return getClient().withExtension(DatabaseDao.class, dao -> dao.find(db) != null);
        }

        public boolean exists(String db, String table)
        {
            return getClient().withExtension(TableDao.class, dao -> dao.find(db, table) != null);
        }

        public String variable(String name)
        {
            Variable variable = getClient().withExtension(VariableDao.class, dao -> dao.find(name));

            return variable == null ? null : variable.value;
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

            Resultset rs = (Resultset)connection.createStatement().executeQuery("SELECT * FROM "+ key + " LIMIT 0");

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

            return new ResultSetFactory(connection, null)
                    .createFromResultsetRows(java.sql.ResultSet.CONCUR_READ_ONLY, java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, newRsRow);
        }

        public ResultSetImpl buildResultSet(List<List<String>> data, String db, String table) throws SQLException, ArrayIndexOutOfBoundsException
        {
            return buildResultSet(data, columnDefinition(db, table));
        }

        public Record mixRecord(String db, String table, Map<String, String> data) throws SQLException
        {
            if (data.isEmpty())
                return null;

            Field[] fields = columnDefinition(db, table).getFields();

            Field[] newFields = new Field[data.keySet().size()];
            List<String> newValue = new ArrayList<>();
            for (Field field: fields
            ) {
                if (!data.containsKey(field.getName()))
                    continue;

                newFields[newValue.size()] = field;
                newValue.add(data.get(field.getName()));
            }

            ColumnDefinition columnDefinition = new DefaultColumnDefinition(newFields);

            ResultSetImpl resultSet = buildResultSet(Arrays.asList(newValue), columnDefinition);

            if (resultSet.next())
            {
                Map<String, Object> row = new HashMap<>();
                for(int i = 1; i <= newFields.length; ++i)
                    row.put(newFields[i - 1].getName(), resultSet.getObject(i));
                return Record.create(table, row);
            }

            return null;
        }

        public Record mixRecord(String db, String table, List<String> data)
        {
            Records records = mixRecords(db, table, Arrays.asList(data));

            return records == null || records.isEmpty() ? null : records.get(0);
        }

        public Records mixRecords(String db, String table, List<List<String>> data)
        {
            try
            {
                ResultSetImpl resultSet = buildResultSet(data, db, table);
                List<String> columnNames = columnNames(db, table);

                Records records = new Records();

                while (resultSet.next())
                {
                    Map<String, Object> row = new HashMap<>();

                    for(int i = 1; i <= columnNames.size(); ++i)
                        row.put(columnNames.get(i - 1), resultSet.getObject(i));

                    records.add(Record.create(table, row));
                }

                return records;

            } catch (SQLException|ArrayIndexOutOfBoundsException e)
            {
                logger.error("Mix records failed.", e);
                return null;
            }
        }

        public Records queryIn(River.TableBase table, String whereInColumn, List<String> values)
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
    }

    public class Connector extends AbstractConnector
    {
        private Jdbi jdbi;
        private ConnectionImpl connection;
        private ConnectionImpl utcConnection;
        public Connector(River river, boolean autoReconnect) {
            super(river, autoReconnect);
        }

        private String getJdbcUrl(String charset, String timeZone, boolean autoReconnect)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("jdbc:mysql://")
                    .append(river.my.host)
                    .append(":")
                    .append(river.my.port)
                    .append("/")
                    .append("INFORMATION_SCHEMA")
                    .append("?useSSL=false&verifyServerCertificate=false")
                    .append("&useUnicode=true&characterEncoding=")
                    .append(charset)
                    .append("&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=true&useTimeZone=true&serverTimezone=")
                    .append(timeZone)
            ;
            if (autoReconnect)
                sb.append("&autoReconnect=true&failOverReadOnly=true");

            return sb.toString();
        }

        @Override
        protected void doConnecting() throws Exception
        {
            if (null != jdbi)
                return;

            String jdbcUrl = getJdbcUrl(river.charset, river.my.timeZone, autoReconnect);
            logger.info("JDBC: {}", jdbcUrl);

            new Driver();
            jdbi = Jdbi.create(jdbcUrl, river.my.user, river.my.password);
            jdbi.installPlugin(new SqlObjectPlugin());
            connection = (ConnectionImpl)jdbi.open().getConnection();
            utcConnection = (ConnectionImpl)DriverManager.getConnection(getJdbcUrl(river.charset, "UTC", false), river.my.user, river.my.password);
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
                 handle.createQuery(SqlUtils.HEARTBEAT).mapTo(Integer.class).findOnly()
            );

            if (i == null || !i.equals(1))
                throw new DisconnectionException("MySQL: Heart beat Fails.");
        }

        @Override
        protected void doClose() throws Exception {
            if (null != connection && !connection.isClosed())
                connection.close();
            if (null != utcConnection && !utcConnection.isClosed())
                utcConnection.close();
        }


        public Jdbi getClient() {
            return jdbi;
        }

        public ConnectionImpl getConnection() {
            return connection;
        }

        public ConnectionImpl getUtcConnection() {
            return utcConnection;
        }
    }
}
