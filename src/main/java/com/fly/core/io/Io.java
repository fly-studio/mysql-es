package com.fly.core.io;

import com.fly.core.text.json.StripJsonComment;
import okio.BufferedSource;
import okio.Okio;

import java.io.File;
import java.io.IOException;

public class Io {

    public static String readUtf8(File file) throws IOException
    {
        BufferedSource source = Okio.buffer(Okio.source(file));
        String str = source.readUtf8();
        source.close();

        return str;
    }

    public static StringBuffer readByte(File file) throws IOException
    {
        BufferedSource source = Okio.buffer(Okio.source(file));
        StringBuffer buffer = new StringBuffer();
        byte[] bytes =  source.readByteArray();
        buffer.append(bytes);
        source.close();

        return buffer;
    }

    public static String readJson(File file) throws IOException
    {
        return StripJsonComment.strip(readUtf8(file));
    }
}
