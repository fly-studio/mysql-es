package com.fly.core.text.json.adapters;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;

import java.io.IOException;
import java.math.BigInteger;

public class BigIntegerAdapter extends JsonAdapter<BigInteger> {

    @Override
    public BigInteger fromJson(JsonReader jsonReader) throws IOException {
        long value = jsonReader.nextLong();
        return BigInteger.valueOf(value);
    }

    @Override
    public void toJson(JsonWriter jsonWriter, BigInteger bigInteger) throws IOException {
        jsonWriter.value(bigInteger);
    }
}
