package com.fly.core.contract;

import com.fly.core.text.StripJsonComment;
import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.adapters.Rfc3339DateJsonAdapter;
import okio.BufferedSink;
import okio.BufferedSource;

import java.io.IOException;
import java.util.Date;

public class AbstractJsonable {

    public static <T> T fromJson(final Class<T> clazz, String json) throws IOException
    {
        JsonAdapter<T> jsonAdapter = makeAdapter(clazz);

        return jsonAdapter.fromJson(StripJsonComment.strip(json));
    }

    public static <T> T fromJson(final Class<T> clazz, BufferedSource source) throws IOException
    {
        JsonAdapter<T> jsonAdapter = makeAdapter(clazz);

        return jsonAdapter.fromJson(source);
    }

    private static <T> JsonAdapter<T> makeAdapter(final Class<T> clazz)
    {
        Moshi moshi = new Moshi.Builder()
                .add(Date.class, new Rfc3339DateJsonAdapter())
                .build();
        return moshi.adapter(clazz);
    }

    public String toJson()
    {
        JsonAdapter jsonAdapter = makeAdapter(this.getClass());
        return jsonAdapter.toJson(this);
    }

    public void toJson(BufferedSink sink) throws IOException
    {
        JsonAdapter jsonAdapter = makeAdapter(this.getClass());
        jsonAdapter.toJson(sink,this);
    }

}
