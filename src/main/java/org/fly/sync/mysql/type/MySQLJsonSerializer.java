package org.fly.sync.mysql.type;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class MySQLJsonSerializer extends StdSerializer<MySQLJson> {

    public MySQLJsonSerializer() {
        this(null);
    }

    public MySQLJsonSerializer(Class<MySQLJson> t) {
        super(t);
    }

    @Override
    public void serialize(MySQLJson value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeRaw(value.getRaw());
    }
}
