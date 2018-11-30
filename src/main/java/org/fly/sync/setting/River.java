package org.fly.sync.setting;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.fly.core.text.json.Jsonable;
import org.fly.sync.Main;
import org.fly.sync.exception.ColumnNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class River extends Jsonable {
    @JsonIgnore
    public final static Logger logger = LoggerFactory.getLogger(Main.class);
    @JsonIgnore
    public static final String NAMESPACE = "::";
    @JsonIgnore
    public static final String DOT = ".";
    @JsonIgnore
    private static final String ASTERISK = "*";

    public Host my;
    public Host es;
    public String charset = "utf8";

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
        return Arrays.asList(relationKey.replaceFirst("^(.*"+NAMESPACE+")", "").split("\\" + DOT));
    }

    public static String makeRelationKey(String tableName, List<String> relationKeys)
    {
        return tableName + NAMESPACE + String.join(DOT, relationKeys);
    }

    public static String makeRelationKey(List<String> relationKeys)
    {
        return String.join(DOT, relationKeys);
    }

    public static String makeRelationKey(String tableName, String relationKeys)
    {
        return tableName + NAMESPACE + relationKeys;
    }

    public static class Host {
        public String host = "";
        public int port = 0;
        public String user = "";
        public String password = "";
        @JsonProperty("time_zone")
        public String timeZone = "UTC";

    }

    public static class Database {
        @JsonProperty("schema")
        public String schemaName = "";
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

                //pad id or other
                table.padColumns();

                // add to associate
                for (Map.Entry<String, River.Relation> relationEntry: table.relations.entrySet()
                ) {

                    if (relationEntry.getKey() == null || relationEntry.getKey().isEmpty())
                        continue;

                    String relationKey = makeRelationKey(tableName, relationEntry.getKey());
                    Relation relation = relationEntry.getValue();

                    relation.schemaName = schemaName;
                    relation.relationKey = relationKey;
                    relation.fixColumns();

                    addAssociate(relationEntry.getValue().tableName, relationKey, table, relation);

                    relation.padColumns();
                }

                // add with to associate;
                for (Map.Entry<String, With> entry1: table.withs.entrySet()
                     ) {
                    String relationKey = entry1.getKey();
                    if (relationKey == null || relationKey.isEmpty()) continue;

                    relationKey = makeRelationKey(tableName, relationKey);

                    setCalledToAssociate(relationKey, table, entry1.getValue());
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

        private void newAssociate(String relatedTableName)
        {
            if (!associates.containsKey(relatedTableName))
                associates.put(relatedTableName, new ArrayList<>());
        }

        private Associate addAssociate(String relatedTableName, String relationKey, Table parentTable, Relation relation)
        {

            return addAssociate(relatedTableName, relationKey, parentTable, Arrays.asList(relation));
        }

        private Associate addAssociate(String relatedTableName, String relationKey, Table parentTable, List<Relation> relationList)
        {
            newAssociate(relatedTableName);

            List<Associate> list = associates.get(relatedTableName);

            Associate related = new Associate(schemaName, relatedTableName, relationKey, parentTable, relationList);

            list.add(related);

            return related;
        }

        private void setCalledToAssociate(String relationKey, Table calledTable, With with)
        {
            List<String> withLeaves = getRelationKeyList(relationKey);

            if (withLeaves.isEmpty())
                return;

            String tableName = calledTable.tableName;
            Associate associate, targetAssociate = null;
            List<Relation> relationList = new ArrayList<>();

            for (int i = 0; i < withLeaves.size(); i++) {
                String leaf = withLeaves.get(i);

                String _relationKey = makeRelationKey(tableName, leaf);
                associate = findAssociate(_relationKey);

                if (associate == null) return;

                relationList.add(associate.getLastRelation());

                tableName = associate.getLastRelation().tableName;

                relationKey = makeRelationKey(calledTable.tableName, withLeaves.subList(0, i + 1));
                if ((targetAssociate = findAssociate(relationKey)) == null)
                    targetAssociate = addAssociate(tableName, relationKey, associate.parentTable, Arrays.asList(relationList.toArray(new Relation[0])));

                targetAssociate.setCalledTable(calledTable);
            }

            // 比如是 comments::user.extra 有兩層
            // 當user_extras更新時只會觸發 comments::user.extra , 而users的更新不會觸發 comments::user
            // 如果需要user的更新也要觸發，則需要將comments::user, comments::user.extra都加入with，并設置同步規則
            targetAssociate.setWith(with);

            // add the middle relation like "comments::user.extra" to user's table and extra's table
            for(int i = 0; i < targetAssociate.nestedRelations.size(); ++i)
            {
                Relation relation = targetAssociate.nestedRelations.get(i);
                List<Associate> associateList = associates.get(relation.tableName);

                if (associateList != null && !associateList.contains(targetAssociate))
                    associateList.add(targetAssociate);
            }
        }

        public boolean hasWith(String tableName)
        {
            return tables.containsKey(tableName) && tables.get(tableName).hasWith();
        }

        public boolean hasTable(String tableName) {
            return tables.containsKey(tableName);
        }

        @JsonIgnore
        public boolean isSync(String tableName)
        {
            return tables.containsKey(tableName) && getTable(tableName).sync;
        }

        public List<Associate> getAssociates(String tableName, boolean unique) {
            List<Associate> associateList = !associates.containsKey(tableName)
                    ? Arrays.asList()
                    : associates.get(tableName)
                        .stream()
                        .filter(associate -> associate.calledTable != null)
                        .collect(Collectors.toList())
                    ;

            return !unique
                    ? associateList
                    : associateList
                        .stream()
                        .filter(associate -> {
                            String key = associate.relationKey + DOT;
                            return associateList
                                    .stream()
                                    .filter(associate1 -> associate1.relationKey.contains(key))
                                    .count() == 0;
                        })
                        .collect(Collectors.toList());
        }

        public List<Associate> getAssociates(String tableName)
        {
            return getAssociates(tableName, true);
        }

    }

    public static class DynamicIndexField {
        private String field;
        private SimpleDateFormat format;
        private int start;
        private int end;

        DynamicIndexField(int start, int end, String field, String format) {
            this.field = field;
            this.format = new SimpleDateFormat(format == null || format.isEmpty() ? "yyyy.MM.dd" : format);
            this.start = start;
            this.end = end;
        }

        public String buildIndexName(String index, Date value)
        {
            return index.substring(0, start) + format.format(value) + index.substring(end);
        }

        public String getField() {
            return field;
        }
    }
    public static class Table extends TableBase {
        public boolean sync = true;
        public String index;
        public String template = "";
        public String type = "_doc";
        public List<String> pk = Arrays.asList("id");
        public Map<String, Relation> relations = new HashMap<>();
        public Map<String, With> withs = new HashMap<>();

        @JsonIgnore
        public List<DynamicIndexField> dynamicIndexFields = new ArrayList<>();

        public Relation getRelation(String name)
        {
            return relations.get(name);
        }

        public boolean hasWith()
        {
            return withs != null && !withs.isEmpty();
        }

        public boolean isDynamicIndexName()
        {
            return !dynamicIndexFields.isEmpty();
        }

        public String buildIndexName(org.fly.sync.mysql.model.Record record)
        {
            if (isDynamicIndexName())
            {
                String newIndex = index;
                for (DynamicIndexField field: Lists.reverse(dynamicIndexFields)
                     ) {
                    Object date = record.get(field.getField(), true);
                    if (date instanceof Date)
                        newIndex = field.buildIndexName(newIndex, (Date)date);
                }

                return newIndex;
            }

            return index;
        }

        void fixName()
        {
            index = index.replace("${SCHEMA}", schemaName).replace("${TABLE}", tableName);
            type = type.replace("${SCHEMA}", schemaName).replace("${TABLE}", tableName);

            // date math
            if (index.charAt(0) == '<' && index.charAt(index.length() - 1) == '>')
            {
                index = index.substring(1, index.length() - 1);
                Matcher m = Pattern.compile("\\{([^\\{\\}\\|]*)(\\|([^\\{\\}]*))?\\}").matcher(index);
                while (m.find())
                {
                    dynamicIndexFields.add(new DynamicIndexField(m.start(), m.end(),  m.group(1), m.group(3)));
                }
            }
        }

        public List<String> getFullWithNames()
        {
            return withs.keySet().stream().map(with -> makeRelationKey(tableName, with)).collect(Collectors.toList());
        }

        void padColumns()
        {
            for (DynamicIndexField field: dynamicIndexFields
                 )
                addColumn(field.getField());

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

    public static class With {
        public Sync sync = new Sync();

        public static class Sync {
            public boolean created = false;
            public boolean updated = true;
            public boolean deleted = true;
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
        @JsonProperty("date_format")
        public String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";

        public void validateColumns(List<String> fullColumns) throws ColumnNotFoundException
        {
            this.fullColumns = fullColumns;

            if (!columns.contains(ASTERISK)) {
                if (columns.retainAll(fullColumns))
                    logger.warn("Table {} columnNames reset to [{}].", tableName, columns);
            }

            columnAlias.entrySet().removeIf(entry -> !fullColumns.contains(entry.getKey()));
        }

        public boolean equals(Object other)
        {
            return other instanceof String ? ((String) other).equalsIgnoreCase(tableName) : super.equals(other);
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
        public With with;

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

        public int findOffset(String tableName) {

            if (calledTable != null && calledTable.equals(tableName)) return 0;

            for (int i = 0; i < nestedRelations.size(); i++) {
                if (nestedRelations.get(i).equals(tableName))
                    return i + 1;
            }

            return -1;
        }

        public Relation getRelation(int offset) {
            return nestedRelations.get(offset);
        }

        public void setWith(With with) {
            this.with = with;
        }

        public With getWith() {
            return with;
        }
    }
}
