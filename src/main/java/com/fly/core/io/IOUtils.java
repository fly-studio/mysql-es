package com.fly.core.io;

import com.fly.core.text.json.StripJsonComment;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.Okio;

import java.io.File;
import java.io.IOException;

public class IOUtils {

    public static String readUtf8(File file) throws IOException
    {
        BufferedSource source = Okio.buffer(Okio.source(file));
        String str = source.readUtf8();
        source.close();

        return str;
    }

    public static void writeUtf8(File file, String content) throws IOException
    {
        BufferedSink sink = Okio.buffer(Okio.sink(file));
        sink.writeUtf8(content);
        sink.close();
    }

    public static StringBuffer readBytes(File file) throws IOException
    {
        BufferedSource source = Okio.buffer(Okio.source(file));
        StringBuffer buffer = new StringBuffer();
        byte[] bytes =  source.readByteArray();
        buffer.append(bytes);
        source.close();

        return buffer;
    }

    public static void writeBytes(File file, byte[] bytes) throws IOException
    {
        BufferedSink sink = Okio.buffer(Okio.sink(file));
        sink.write(bytes);
        sink.close();
    }

    public static String readJson(File file) throws IOException
    {
        return StripJsonComment.strip(readUtf8(file));
    }
}
