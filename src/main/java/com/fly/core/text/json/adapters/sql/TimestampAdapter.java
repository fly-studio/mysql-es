package com.fly.core.text.json.adapters.sql;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;

import java.io.IOException;
import java.sql.Timestamp;

public class TimestampAdapter extends JsonAdapter<Timestamp> {
    @Override
    public Timestamp fromJson(JsonReader jsonReader) throws IOException {
        long timestamp = jsonReader.nextLong();
        return new Timestamp(timestamp);
    }

    @Override
    public void toJson(JsonWriter jsonWriter, Timestamp timestamp) throws IOException {
        jsonWriter.value(timestamp == null ? null : timestamp.toString());
    }
}
