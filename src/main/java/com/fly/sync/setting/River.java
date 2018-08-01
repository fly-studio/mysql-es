package com.fly.sync.setting;

import com.fly.core.text.json.Jsonable;
import com.squareup.moshi.Json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class River extends Jsonable {

    public Host my;
    public Host es;
    public String charset = "utf8";
    @Json(name = "server_id") public int serverId = 9999;

    public List<Database> databases;

    public Database getDatabase(String db)
    {
        for (Database database: databases
             ) {
            if (database.db.equals(db))
                return database;
        }
        return null;
    }

    public void init()
    {
        for(River.Database database : databases)
            database.init();

    }

    private static String replaceDBValue(String str, String db, String table)
    {
        return str.replace("${DB}", db).replace("${TABLE}", table);
    }

    public enum SyncType {
        always,
        once
    }

    public static class Host {
        public String host = "";
        public int port = 0;
        public String user = "";
        public String password = "";
    }

    public static class Database {
        public String db = "";
        public Map<String, Table> tables = new HashMap<>();

        // set in init
        // relationKey => Relation
        public Map<String, Relation> relations = new HashMap<>();
        // table => [...relationKey];
        public Map<String, List<String>> tablesBeRelated = new HashMap<>();

        public Table getTable(String name)
        {
            return tables.get(name);
        }

        private void init()
        {
            for(Map.Entry<String, River.Table> entry: tables.entrySet())
            {
                String tableName = entry.getKey();
                River.Table table = entry.getValue();
                table.index = replaceDBValue(table.index, db, tableName);
                table.type = replaceDBValue(table.type, db, tableName);

                putTableBeRelated(tableName, null);

                // add full name of relations
                for (Map.Entry<String, River.Relation> relationEntry: table.relations.entrySet()
                ) {
                    String relationKey = tableName + "." + relationEntry.getKey();
                    relations.put(relationKey, relationEntry.getValue());
                    putTableBeRelated(relationEntry.getValue().table, relationKey);
                }

                // modify to full name of withs
                Map<String, River.SyncType> temp = new HashMap<>();
                for (Map.Entry<String, River.SyncType> withEntry: table.with.entrySet()
                ) {
                    String key = withEntry.getKey();
                    if (key != null && !key.contains("."))
                        key = tableName + "." + key;

                    temp.put(key, withEntry.getValue());
                }
                table.with.clear();
                table.with.putAll(temp);

            }
        }

        protected void putTableBeRelated(String tableName, String relationKey)
        {
            if (!tablesBeRelated.containsKey(tableName))
                tablesBeRelated.put(tableName, new ArrayList<>());

            List<String> list = tablesBeRelated.get(tableName);

            if (null == relationKey || list.contains(relationKey)) {
                return;
            }

            list.add(relationKey);

        }
    }

    public static class Table {
        public boolean sync = true;
        public String index;
        public String template = "";
        public String type = "_doc";
        public String[] id = new String[] {"id"};
        public String[] columns = new String[] {"*"};
        @Json(name = "column_alias") public Map<String, String> columnAlias = new HashMap<String, String>();
        public Map<String, Relation> relations = new HashMap<String, Relation>();
        public Map<String, SyncType> with = new HashMap<String, SyncType>();

        public Relation getRelation(String name)
        {
            return relations.get(name);
        }

        public SyncType getWith(String name)
        {
            return with.get(name);
        }
    }

    public static class Relation {
        public String table;
        public String foreign;
        public String local;
        public String[] columns = new String[] {"*"};
        @Json(name = "column_alias") public Map<String, String> columnAlias = new HashMap<String, String>();

    }
}
