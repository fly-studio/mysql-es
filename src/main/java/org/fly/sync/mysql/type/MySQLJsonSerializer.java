package org.fly.sync.mysql.type;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class MySQLJsonSerializer extends JsonSerializer<MySQLJson> {

    @Override
    public void serialize(MySQLJson value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        String raw = value.getRaw();
        if (raw == null || raw.isEmpty())
            gen.writeNull();
        else
            gen.writeString(raw);
            //gen.writeRawValue(raw);
    }
}
