package com.fly.sync.setting;

import com.fly.utils.text.StripJsonComment;
import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import com.sun.istack.internal.NotNull;
import okio.BufferedSource;
import okio.Okio;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

public class Setting {

    public static String CONFIG_PATH = null;

    public static File getEtc()
    {
        return new File (System.getProperty("user.dir"), "etc");
    }

    public static File getEtc(@NotNull String filename)
    {
        return new File(getEtc(), filename);
    }

    public static String readJson(String path) throws IOException
    {
        File file = new File(path);
        BufferedSource source = Okio.buffer(Okio.source(file));
        String str = source.readUtf8();
        source.close();

        return StripJsonComment.strip(str);
    }

    public static <T> T readJson(String path, Type clazz) throws IOException
    {
        Moshi moshi = new Moshi.Builder().build();
        JsonAdapter<T> jsonAdapter = moshi.adapter(clazz);

        return jsonAdapter.fromJson(readJson(path));
    }

    public static River readRiver() throws IOException
    {
        return readRiver(Setting.CONFIG_PATH);
    }

    public static River readRiver(String path) throws IOException
    {
        River river = Setting.readJson(path, River.class);
        for(River.Database db : river.databases)
        {
            for(Map.Entry<String, River.Table> entry: db.tables.entrySet())
            {
                String dbName = db.db;
                River.Table table = entry.getValue();
                table.index = replaceDBValue(table.index, dbName, entry.getKey());
                table.type = replaceDBValue(table.type, dbName, entry.getKey());

            }
        }
        return river;
    }

    private static String replaceDBValue(String str, String db, String table)
    {
        return str.replace("${DB}", db).replace("${TABLE}", table);
    }
}
