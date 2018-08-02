package com.fly.sync.setting;

import com.fly.core.text.json.Jsonable;
import com.fly.sync.Main;
import com.squareup.moshi.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class River extends Jsonable {
    public final static Logger logger = LoggerFactory.getLogger(Main.class);

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
        // table => [...{relationKey, relation, parentTable, with}];
        public Map<String, List<Associate>> associates = new HashMap<>();

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
                table.tableName = tableName;
                table.index = replaceDBValue(table.index, db, tableName);
                table.type = replaceDBValue(table.type, db, tableName);

                table.fixAsteriskColumns();
                addAssociate(tableName);

                // add to associate
                for (Map.Entry<String, River.Relation> relationEntry: table.relations.entrySet()
                ) {

                    if (relationEntry.getKey() == null || relationEntry.getKey().isEmpty())
                        continue;

                    String relationKey = tableName + "." + relationEntry.getKey();
                    Relation relation = relationEntry.getValue();
                    relation.fixAsteriskColumns();

                    addAssociate(relationEntry.getValue().tableName, relationKey, table, relation);
                }

                // add with to associate
                for (Map.Entry<String, River.SyncType> withEntry: table.with.entrySet()
                ) {
                    String relationKey = withEntry.getKey();
                    if (relationKey == null) continue;

                    if (!relationKey.contains("."))
                        relationKey = tableName + "." + relationKey;

                    addWithToAssociate(relationKey, table, withEntry.getValue());
                }

            }
        }

        public Associate findAssociate(String relationKey)
        {
            for (Map.Entry<String, List<Associate>> entry: associates.entrySet()
                 ) {
                for (Associate associates: entry.getValue()
                     ) {
                    if (associates.relationKey.equals(relationKey))
                        return associates;
                }
            }
            return null;
        }

        protected void addAssociate(String relatedTableName)
        {
            if (!associates.containsKey(relatedTableName))
                associates.put(relatedTableName, new ArrayList<>());
        }

        protected void addAssociate(String relatedTableName, String relationKey, Table parentTable, Relation relation)
        {
            addAssociate(relatedTableName);

            List<Associate> list = associates.get(relatedTableName);

            Associate related = new Associate(relationKey, parentTable, relation);

            list.add(related);
        }

        protected void addWithToAssociate(Associate associate, Table parentTable, SyncType syncType)
        {
            associate.addWith(parentTable, syncType);
        }

        protected void addWithToAssociate(String relationKey, Table parentTable, SyncType syncType)
        {
            Associate associate = findAssociate(relationKey);
            if (associate != null)
                addWithToAssociate(associate, parentTable, syncType);
        }
    }

    public static class Table extends TableBase {
        public boolean sync = true;
        public String index;
        public String template = "";
        public String type = "_doc";
        public String[] id = new String[] {"id"};
        public Map<String, Relation> relations = new HashMap<>();
        public Map<String, SyncType> with = new HashMap<>();

        public Relation getRelation(String name)
        {
            return relations.get(name);
        }

        public SyncType getWith(String name)
        {
            return with.get(name);
        }
    }

    public static class Relation extends TableBase {
        public String foreign;
        public String local;
    }

    private static class TableBase {
        @Json(name = "table") public String tableName;
        public List<String> columns = Arrays.asList("*");
        @Json(name = "column_alias") public Map<String, String> columnAlias = new HashMap<>();

        public void fixColumns(List<String> allColumns)
        {
            if (!columns.contains("*")) {
                if (columns.retainAll(allColumns))
                    logger.warn("Table {} columns reset to {}.", tableName, columns);
            }

            columnAlias.entrySet().removeIf(entry -> !allColumns.contains(entry.getKey()));
        }

        public void fixAsteriskColumns()
        {
            if (columns.isEmpty() || (columns.contains("*") && columns.size() != 1)) {
                columns = Arrays.asList("*");
                logger.warn("Table {} columns reset to [\"*\"].", tableName);
            }
            if (columnAlias.size() > 0)
                columnAlias.entrySet().removeIf(entry -> entry.getKey().equals("*") || entry.getKey().equals(entry.getValue()));
        }

    }

    public static class Associate {
        String relationKey;
        Table parentTable;
        Relation relation;
        List<With> withs = new ArrayList<>();

        public Associate(String relationKey, Table parentTable, Relation relation) {
            this.relationKey = relationKey;
            this.parentTable = parentTable;
            this.relation = relation;
        }

        public void addWith(Table parentTable, SyncType syncType)
        {
            withs.add(new With(relationKey, parentTable, syncType));
        }
    }

    public static class With {
        String relationKey;
        SyncType syncType;
        Table parentTable;

        public With(String relationKey, Table parentTable, SyncType syncType) {
            this.relationKey = relationKey;
            this.syncType = syncType;
            this.parentTable = parentTable;
        }
    }
}
