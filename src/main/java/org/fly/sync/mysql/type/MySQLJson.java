package org.fly.sync.mysql.type;

public class MySQLJson {

    private String raw;

    public MySQLJson(String json) {
        this.raw = json;
    }

    public String getRaw() {
        return raw;
    }
}
