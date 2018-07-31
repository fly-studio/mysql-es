package com.fly.sync.mysql;

import com.fly.sync.contract.AbstractConnector;
import com.fly.sync.setting.River;
import com.mysql.jdbc.Driver;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySql  {

    public Connector connector;
    private River river;

    public MySql(River river, River.Database database) {
        this.river = river;
        connector = new Connector(river, database, true);
    }

    public Connection getConnection()
    {
        return connector.getConnection();
    }

    protected PreparedStatement newStatement(String sql, String... params) throws SQLException
    {
        PreparedStatement statement = getConnection().prepareStatement(sql);

        for (int i = 0; i < params.length; ++i)
            statement.setString(i, params[i]);

        return statement;
    }

    public void execute(String sql, String... params) throws SQLException
    {
        PreparedStatement statement = newStatement(sql, params);
        statement.execute();

        statement.close();
    }

    public List<Map<String, String>> executeQuery(String sql, String... params) throws SQLException
    {
        PreparedStatement statement = newStatement(sql, params);
        ResultSet rs = statement.executeQuery();

        return fetchArray(rs);
    }

    public List<Map<String, String>> fetchArray(ResultSet rs)
    {
        List<String> columns = fetchColumns(rs);
        List<Map<String, String>> records = new ArrayList<>();

        if (columns.isEmpty())
            return records;
        try {
            while (rs.next())
            {
                Map<String, String> line = new HashMap<>();
                for (String col: columns
                     ) {
                    line.put(col, rs.getString(col));
                }
                records.add(line);
            }
        } catch (Exception e)
        {

        }

        return records;
    }

    public List<String> fetchColumns(ResultSet rs)
    {
        List<String> columns = new ArrayList<>();
        try
        {
            ResultSetMetaData metaData = rs.getMetaData();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                columns.add(metaData.getColumnName(i));
            }
        } catch (Exception e)
        {

        }
        return columns;
    }

    public class Connector extends AbstractConnector
    {
        private Connection connection;
        private River.Database database;

        public Connector(River river, River.Database database, boolean autoReconnect) {
            super(river, autoReconnect);
            this.database = database;
        }

        @Override
        protected void doConnecting() {
            StringBuilder sb = new StringBuilder();
            sb.append("jdbc:mysql://")
                .append(river.my.host)
                .append(":")
                .append(river.my.port)
                .append("/")
                .append(database.db);

            try {
                new Driver();

                connection = DriverManager.getConnection(sb.toString(), river.my.user, river.my.password);

            } catch (SQLException e) {

            }
        }

        @Override
        protected void doReconnect() {

        }

        @Override
        protected void doHeartbeat() throws Exception
        {
            Statement statement = connection.createStatement();
            statement.execute("SELECT 1;");
            statement.close();
        }

        @Override
        protected void doClose() throws Exception {

            connection.close();
        }

        public Connection getConnection() {
            return connection;
        }
    }
}
