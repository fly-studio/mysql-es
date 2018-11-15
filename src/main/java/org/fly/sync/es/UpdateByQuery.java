package org.fly.sync.es;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.fly.core.text.json.Jsonable;

import java.util.HashMap;
import java.util.Map;

public class UpdateByQuery extends Jsonable {
    public Query query = new Query();
    public Script script = new Script();

    public static class Query {

        public Map<String, Object> term = new HashMap<>();

        @JsonIgnore
        public void setField(String key, Object value){
            term.put(key, value);
        }

    }

    public static class Script {
        public String lang = "painless";
        public Map<String, Object> params;


        public String getSource()
        {
            if (null == params) return "";

            StringBuilder sb = new StringBuilder();
            for (String key: params.keySet()
                 ) {
                sb.append("ctx._source[\"")
                        .append(key)
                        .append("\"]")
                        .append("=params[\"")
                        .append(key)
                        .append("\"];");
            }
            return sb.toString();
        }

    }
}
