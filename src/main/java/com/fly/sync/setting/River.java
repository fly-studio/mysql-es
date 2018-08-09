package com.fly.sync.setting;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fly.core.text.json.Jsonable;
import com.fly.sync.Main;
import com.fly.sync.exception.ColumnNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class River extends Jsonable {
    @JsonIgnore
    public final static Logger logger = LoggerFactory.getLogger(Main.class);
    @JsonIgnore
    private static final String NAMESPACE = "::";
    @JsonIgnore
    private static final String ASTERISK = "*";

    public Host my;
    public Host es;
    public String charset = "utf8";
    @JsonProperty("server_id")
    public int serverId = 9999;

    public List<Database> databases;

    public Database getDatabase(String db)
    {
        for (Database database: databases
             ) {
            if (database.schemaName.equals(db))
                return database;
        }
        return null;
    }

    public void init()
    {
        for(River.Database database : databases)
            database.init();

    }

    public static List<String> getRelationKeyList(String relationKey)
    {
        return Arrays.asList(relationKey.replaceFirst("^(.*"+NAMESPACE+")", "").split("\\."));
    }

    public static class Host {
        public String host = "";
        public int port = 0;
        public String user = "";
        public String password = "";
    }

    public static class Database {
        @JsonProperty("schema") public String schemaName = "";
        public Map<String, Table> tables = new HashMap<>();

        // set in init
        // table => [...{relationKey, relation, parentTable, with}];
        @JsonIgnore
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
                table.schemaName = schemaName;
                table.tableName = tableName;
                table.fixName();
                table.fixColumns();

                newAssociate(tableName);

                table.padColumns();

                // add to associate
                for (Map.Entry<String, River.Relation> relationEntry: table.relations.entrySet()
                ) {

                    if (relationEntry.getKey() == null || relationEntry.getKey().isEmpty())
                        continue;

                    String relationKey = tableName + NAMESPACE + relationEntry.getKey();
                    Relation relation = relationEntry.getValue();

                    relation.schemaName = schemaName;
                    relation.relationKey = relationKey;
                    relation.fixColumns();

                    addAssociate(relationEntry.getValue().tableName, relationKey, table, relation);

                    relation.padColumns();
                }

                // add with to associate;
                for (int i = 0; i < table.withs.size(); ++i) {
                    String relationKey = table.withs.get(i);
                    if (relationKey == null) continue;

                    relationKey = tableName + NAMESPACE + relationKey;

                    setCalledToAssociate(relationKey, table);
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

        void newAssociate(String relatedTableName)
        {
            if (!associates.containsKey(relatedTableName))
                associates.put(relatedTableName, new ArrayList<>());
        }

        Associate addAssociate(String relatedTableName, String relationKey, Table parentTable, Relation relation)
        {

            return addAssociate(relatedTableName, relationKey, parentTable, Arrays.asList(relation));
        }

        Associate addAssociate(String relatedTableName, String relationKey, Table parentTable, List<Relation> relationList)
        {
            newAssociate(relatedTableName);

            List<Associate> list = associates.get(relatedTableName);

            Associate related = new Associate(schemaName, relatedTableName, relationKey, parentTable, relationList);

            list.add(related);

            return related;
        }

        void setCalledToAssociate(String relationKey, Table calledTable)
        {
            List<String> withLeaves = getRelationKeyList(relationKey);

            if (withLeaves.isEmpty())
                return;

            String tableName = calledTable.tableName;
            Associate associate = null;
            List<Relation> relationList = new ArrayList<>();

            for (String leaf : withLeaves
                 ) {

                String _relationKey = tableName + NAMESPACE + leaf;
                associate = findAssociate(_relationKey);

                if (associate == null) return;

                relationList.add(associate.getLastRelation());

                tableName = associate.getLastRelation().tableName;
            }

            if (findAssociate(relationKey) == null)
                associate = addAssociate(tableName, relationKey, associate.parentTable, relationList);

            associate.setCalledTable(calledTable);

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
        public List<String> pk = Arrays.asList("id");
        public Map<String, Relation> relations = new HashMap<>();
        public List<String> withs = new ArrayList<>();

        public Relation getRelation(String name)
        {
            return relations.get(name);
        }

        public boolean hasWith()
        {
            return withs != null && !withs.isEmpty();
        }

        void fixName()
        {
            index = index.replace("${SCHEMA}", schemaName).replace("${TABLE}", tableName);
            type = type.replace("${SCHEMA}", schemaName).replace("${TABLE}", tableName);
        }

        public List<String> getFullWiths()
        {
            List<String> withNames = new ArrayList<>();
            for (String with: withs
                 ) {
                withNames.add(tableName + NAMESPACE + with);
            }
            return withNames;
        }

        void padColumns()
        {
            for (String _id: pk)
                addColumn(_id);

            for (Map.Entry<String, Relation> relationEntry: relations.entrySet())
                addColumn(relationEntry.getValue().local);
        }

        @Override
        public void validateColumns(List<String> fullColumns) throws ColumnNotFoundException
        {
            super.validateColumns(fullColumns);

            if (pk == null || pk.isEmpty())
                throw new ColumnNotFoundException("Empty Primary key, in Table [" + tableName +"] of Database ["+ schemaName +"]");

            for(String key: pk)
                if (!fullColumns.contains(key))
                    throw new ColumnNotFoundException("Primary key column ["+ key +"] not found in Table [" + tableName +"] of Database ["+ schemaName +"]");

            for (Map.Entry<String, Relation> entry: relations.entrySet()
                 ) {
                String local = entry.getValue().local;
                if (!fullColumns.contains(local))
                    throw new ColumnNotFoundException("Local column ["+ local +"] not found in Relation [" + entry.getValue().relationKey +"] of Database ["+ schemaName +"]");
            }
        }
    }

    public static class Relation extends TableBase {
        @JsonIgnore
        public String relationKey;
        public String foreign;
        public String local;

        void padColumns()
        {
            addColumn(foreign);
        }

        public void validateColumns(List<String> fullColumns) throws ColumnNotFoundException
        {
            super.validateColumns(fullColumns);

            if (!fullColumns.contains(foreign))
                throw new ColumnNotFoundException("Foreign column ["+ foreign +"] not found in Relation ["+ relationKey +"] of Database ["+ schemaName +"]");
        }
    }

    public static class TableBase {
        @JsonIgnore
        public String schemaName;
        @JsonProperty("table")
        public String tableName;
        public List<String> columns = Arrays.asList(ASTERISK);
        @JsonIgnore
        public List<String> fullColumns;
        @JsonProperty("column_alias")
        public Map<String, String> columnAlias = new HashMap<>();

        public void validateColumns(List<String> fullColumns) throws ColumnNotFoundException
        {
            this.fullColumns = fullColumns;

            if (!columns.contains(ASTERISK)) {
                if (columns.retainAll(fullColumns))
                    logger.warn("Table {} columnNames reset to [{}].", tableName, columns);
            }

            columnAlias.entrySet().removeIf(entry -> !fullColumns.contains(entry.getKey()));
        }

        @JsonIgnore
        public List<String> getColumns()
        {
            return columns.contains(ASTERISK) ? fullColumns : columns;
        }

        void addColumn(String column)
        {
            if (columns.contains(ASTERISK))
                return;

            if (!columns.contains(column))
                columns.add(column);
        }

        void fixColumns()
        {
            if (columns.isEmpty() || (columns.contains(ASTERISK) && columns.size() != 1)) {
                columns = Arrays.asList(ASTERISK);
                logger.warn("Table {} columnNames reset to [\"*\"].", tableName);
            }

            if (columnAlias.size() > 0)
                columnAlias.entrySet().removeIf(entry -> entry.getKey().equals(ASTERISK) || entry.getKey().equals(entry.getValue()));
        }

        public String toSql()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ")
                .append(String.join(", ", columns))
                .append(" FROM `")
                .append(schemaName)
                .append("`.`")
                .append(tableName)
                .append("` ");

            return sb.toString();
        }

        public String toSql(String whereInColumn)
        {
            return toSql() + " WHERE `" + whereInColumn +"` IN (<Values>)";
        }

    }

    public static class Associate {
        public String schemaName;
        public String tableName;
        public String relationKey;
        public List<String> columns;
        public Table parentTable;
        public Table calledTable;
        public List<Relation> nestedRelations;

        public Associate(String schemaName, String tableName, String relationKey, Table parentTable, List<Relation> relationList) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.relationKey = relationKey;
            this.parentTable = parentTable;
            this.nestedRelations = relationList;
        }

        public List<String> getRelationKeyList()
        {
            return River.getRelationKeyList(relationKey);
        }

        public Relation getLastRelation()
        {
            return nestedRelations.get(nestedRelations.size() - 1);
        }

        public Relation getFirstRelation()
        {
            return nestedRelations.get(0);
        }

        public void setCalledTable(Table calledTable)
        {
           this.calledTable = calledTable;
        }
    }
}
