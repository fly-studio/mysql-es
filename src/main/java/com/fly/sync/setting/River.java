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
                newAssociate(tableName);

                // add to associate
                for (Map.Entry<String, River.Relation> relationEntry: table.relations.entrySet()
                ) {

                    if (relationEntry.getKey() == null || relationEntry.getKey().isEmpty())
                        continue;

                    String relationKey = tableName + "::" + relationEntry.getKey();
                    Relation relation = relationEntry.getValue();
                    relation.fixAsteriskColumns();

                    addAssociate(relationEntry.getValue().tableName, relationKey, table, relation);
                }

                // add with to associate
                Map<String, River.SyncType> tmp = new HashMap<>();
                for (Map.Entry<String, River.SyncType> withEntry: table.withs.entrySet()
                ) {
                    String relationKey = withEntry.getKey();
                    if (relationKey == null) continue;

                    relationKey = tableName + "::" + relationKey;

                    tmp.put(relationKey, withEntry.getValue());

                    setCalledToAssociate(relationKey, table, withEntry.getValue());
                }

                table.withs.clear();
                table.withs.putAll(tmp);

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

        protected void newAssociate(String relatedTableName)
        {
            if (!associates.containsKey(relatedTableName))
                associates.put(relatedTableName, new ArrayList<>());
        }

        protected Associate addAssociate(String relatedTableName, String relationKey, Table parentTable, Relation relation)
        {

            return addAssociate(relatedTableName, relationKey, parentTable, Arrays.asList(relation));
        }

        protected Associate addAssociate(String relatedTableName, String relationKey, Table parentTable, List<Relation> relationList)
        {
            newAssociate(relatedTableName);

            List<Associate> list = associates.get(relatedTableName);

            Associate related = new Associate(relatedTableName, relationKey, parentTable, relationList);

            list.add(related);

            return related;
        }


        protected void setCalledToAssociate(String relationKey, Table calledTable, SyncType syncType)
        {
            List<String> withLeaves = Arrays.asList(relationKey.replaceFirst("^(.*::)", "").split("\\."));

            if (withLeaves.isEmpty())
                return;

            String tableName = calledTable.tableName;
            Associate associate = null;
            List<Relation> relationList = new ArrayList<>();

            for (String leaf : withLeaves
                 ) {

                String _relationKey = tableName + "::" + leaf;
                associate = findAssociate(_relationKey);

                if (associate == null) return;

                relationList.add(associate.getLastRelation());

                tableName = associate.getLastRelation().tableName;
            }

            if (findAssociate(relationKey) == null)
                associate = addAssociate(tableName, relationKey, associate.parentTable, relationList);

            associate.setCalledTable(calledTable, syncType);

        }

        public boolean hasWith(String tableName)
        {
            return tables.containsKey(tableName) && tables.get(tableName).hasWith();
        }
    }

    public static class Table extends TableBase {
        public boolean sync = true;
        public String index;
        public String template = "";
        public String type = "_doc";
        public String[] id = new String[] {"id"};
        public Map<String, Relation> relations = new HashMap<>();
        public Map<String, SyncType> withs = new HashMap<>();

        public Relation getRelation(String name)
        {
            return relations.get(name);
        }

        public SyncType getWith(String name)
        {
            return withs != null ? withs.get(name) : null;
        }

        public List<String> getWithNames()
        {
            return withs != null ? Arrays.asList(withs.keySet().toArray(new String[0])) : null;
        }

        public boolean hasWith()
        {
            return withs != null && !withs.isEmpty();
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
        String tableName;
        String relationKey;
        Table parentTable;
        Table calledTable;
        SyncType syncType;
        List<Relation> relationNested = new ArrayList<>();

        public Associate(String tableName, String relationKey, Table parentTable, Relation relation) {
            this.tableName = tableName;
            this.relationKey = relationKey;
            this.parentTable = parentTable;
            this.relationNested.add(relation);
        }

        public Associate(String tableName, String relationKey, Table parentTable, List<Relation> relationList) {
            this.tableName = tableName;
            this.relationKey = relationKey;
            this.parentTable = parentTable;
            this.relationNested = relationList;
        }

        public Relation getLastRelation()
        {
            return relationNested.get(relationNested.size() - 1);
        }

        public void setCalledTable(Table calledTable, SyncType syncType)
        {
           this.calledTable = calledTable;
           this.syncType = syncType;
        }
    }

    public static class With {
        String relationKey;
        SyncType syncType;
        Table calledTable;

        public With(String relationKey, Table calledTable, SyncType syncType) {
            this.relationKey = relationKey;
            this.syncType = syncType;
            this.calledTable = calledTable;
        }
    }
}
