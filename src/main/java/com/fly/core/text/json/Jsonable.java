package com.fly.core.text.json;

import com.fly.core.text.json.adapters.FileAdapter;
import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.adapters.Rfc3339DateJsonAdapter;
import okio.BufferedSink;
import okio.BufferedSource;

import java.io.File;
import java.io.IOException;
import java.util.Date;

public class Jsonable {

    static class Builder{
        private static <T> JsonAdapter<T> makeAdapter(final Class<T> clazz)
        {
            Moshi moshi = new Moshi.Builder()
                    .add(File.class, new FileAdapter())
                    .add(Date.class, new Rfc3339DateJsonAdapter())
                    .build();
            return moshi.adapter(clazz);
        }
    }

    public static <T> T fromJson(final Class<T> clazz, String json) throws IOException
    {
        JsonAdapter<T> jsonAdapter = Builder.makeAdapter(clazz);

        return jsonAdapter.fromJson(StripJsonComment.strip(json));
    }

    public static <T> T fromJson(final Class<T> clazz, BufferedSource source) throws IOException
    {
        JsonAdapter<T> jsonAdapter = Builder.makeAdapter(clazz);

        return jsonAdapter.fromJson(source);
    }

    public String toJson()
    {
        JsonAdapter jsonAdapter = Builder.makeAdapter(this.getClass());
        return jsonAdapter.toJson(this);
    }

    public void toJson(BufferedSink sink) throws IOException
    {
        JsonAdapter jsonAdapter = Builder.makeAdapter(this.getClass());
        jsonAdapter.toJson(sink,this);
    }

}
