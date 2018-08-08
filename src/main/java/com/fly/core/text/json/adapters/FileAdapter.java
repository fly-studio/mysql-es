package com.fly.core.text.json.adapters;

import com.squareup.moshi.*;

import java.io.File;
import java.io.IOException;

public class FileAdapter extends JsonAdapter<File> {

    @ToJson
    public synchronized File fromJson(JsonReader reader) throws IOException
    {
        String string = reader.nextString();
        return string == null || string.length() == 0 ? null : new File(string);
    }

    @FromJson
    public synchronized void toJson(JsonWriter writer, File file) throws IOException {
        writer.value(file == null ? null : file.getAbsolutePath());
    }
}