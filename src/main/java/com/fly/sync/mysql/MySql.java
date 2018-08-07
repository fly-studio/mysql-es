package com.fly.sync.mysql;

import com.fly.sync.contract.AbstractConnector;
import com.fly.sync.exception.ColumnNotFoundException;
import com.fly.sync.exception.DisconnectionException;
import com.fly.sync.exception.RecordNotFoundException;
import com.fly.sync.mysql.model.*;
import com.fly.sync.setting.River;
import com.mysql.cj.jdbc.Driver;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MySql  {

    public Connector connector;
    private River river;
    private Map<String, List<String>> columnsCache = new HashMap<>();

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

    public List<String> columns(String db, String table, boolean force)
    {
        String key = db + "." + table;
        if (!force)
        {
            if (columnsCache.containsKey(key))
                return columnsCache.get(key);
        }

        List<String> list = getClient().withExtension(ColumnDao.class, dao -> dao.allNames(db, table));
        columnsCache.put(key, list);
        return list;
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
            //connection = jdbi.open();
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


        }


        public Jdbi getClient() {
            return jdbi;
        }
    }
}
