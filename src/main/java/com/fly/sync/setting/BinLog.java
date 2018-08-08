package com.fly.sync.setting;

import com.fly.core.text.json.Jsonable;

import java.util.HashMap;
import java.util.Map;

public class BinLog extends Jsonable {
    public Map<String, Position> lists = new HashMap<>();

    public boolean isEmpty(String db)
    {
        Position position = get(db);
        return position == null || position.isEmpty();
    }

    public void set(String db, Position position)
    {
        synchronized (BinLog.class) {
            lists.put(db, position);
        }
    }

    public Position get(String db)
    {
        synchronized (BinLog.class) {
            return lists.get(db);
        }
    }

    public void remove(String db)
    {
        lists.remove(db);
    }

    public static class Position {
        public String name = "";
        public long position = 0;

        public Position()
        {

        }

        public Position(String name, long position) {
            this.name = name;
            this.position = position;
        }

        public boolean isEmpty()
        {
            return name == null || name.isEmpty();
        }

        public void reset()
        {
            name = "";
            position = 0;
        }

        public void updateFrom(Position position) {
            this.name = position.name;
            this.position = position.position;
        }
    }

}
