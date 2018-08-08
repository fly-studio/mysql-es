package com.fly.core.text.json;

import com.fly.core.contract.AbstractJsonable;
import com.fly.core.text.json.adapters.BigIntegerAdapter;
import com.fly.core.text.json.adapters.FileAdapter;
import com.fly.core.text.json.adapters.sql.TimestampAdapter;
import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.adapters.Rfc3339DateJsonAdapter;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.Okio;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

public class Moshiable implements AbstractJsonable {

    public static class Builder{
        public static <T> JsonAdapter<T> makeAdapter(final Class<T> clazz)
        {
            Moshi moshi = new Moshi.Builder()
                    .add(File.class, new FileAdapter())
                    .add(Date.class, new Rfc3339DateJsonAdapter())
                    .add(BigInteger.class, new BigIntegerAdapter())
                    .add(Timestamp.class, new TimestampAdapter())
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

    @Override
    public String toJson()
    {
        JsonAdapter jsonAdapter = Builder.makeAdapter(getClass());
        return jsonAdapter.toJson(this);
    }

    public void toJson(BufferedSink sink) throws IOException
    {
        JsonAdapter jsonAdapter = Builder.makeAdapter(getClass());
        jsonAdapter.toJson(sink,this);
    }

    @Override
    public void toJson(File file) throws Exception {
        JsonAdapter jsonAdapter = Builder.makeAdapter(getClass());
        BufferedSink sink = Okio.buffer(Okio.sink(file));
        jsonAdapter.toJson(sink, this);
        sink.close();
    }
}
