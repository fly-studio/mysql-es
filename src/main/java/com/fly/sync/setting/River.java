package com.fly.sync.setting;

import com.fly.core.text.json.Jsonable;
import com.squareup.moshi.Json;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class River extends Jsonable {

    public Host my;
    public Host es;
    public String charset = "utf8";
    @Json(name = "server_id") public int serverId = 9999;

    public List<Database> databases;

    enum SyncType {
        always,
        once
    }

    static class Host {
        public String host = "";
        public int port = 0;
        public String user = "";
        public String password = "";
    }

    static class Database {
        public String db = "";
        public Map<String, Table> tables = new HashMap<String, Table>();
    }

    static class Table {
        public boolean sync = true;
        public String index;
        public String template = "";
        public String type = "_doc";
        public String[] id = new String[] {"id"};
        public String[] columns = new String[] {"*"};
        @Json(name = "column_alias") public Map<String, String> columnAlias = new HashMap<String, String>();
        public Map<String, Relation> relations = new HashMap<String, Relation>();
        public Map<String, SyncType> with = new HashMap<String, SyncType>();
    }

    static class Relation {
        public String table;
        public String foreign;
        public String local;
        public String[] columns = new String[] {"*"};
        @Json(name = "column_alias") public Map<String, String> columnAlias = new HashMap<String, String>();

    }
}
