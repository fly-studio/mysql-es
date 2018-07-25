package com.fly.sync.setting;

import com.fly.core.contract.AbstractJsonable;
import com.fly.core.text.StripJsonComment;
import com.sun.istack.internal.NotNull;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.Okio;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class Setting {

    public static String ETC_PATH = null;

    public final static String RIVER_FILE = "river.json";
    public final static String CONFIG_FILE = "config.json";
    public final static String BINLOG_FILE = "binlog.json";


    public static Config config;
    public static River river;
    public static BinLog binLog;

    public static File getEtc()
    {
        return new File (System.getProperty("user.dir"), "etc");
    }

    public static File getEtc(@NotNull String filename)
    {
        return new File(getEtc(), filename);
    }

    public static String readJson(File file) throws IOException
    {
        BufferedSource source = Okio.buffer(Okio.source(file));
        String str = source.readUtf8();
        source.close();
        return StripJsonComment.strip(str);
    }

    public static Config getConfig()
    {
        return getConfig(new File(Setting.ETC_PATH, CONFIG_FILE));
    }

    public static Config getConfig(File file)
    {
        if (Setting.config != null)
            return Setting.config;

        Config config;
        try {
            config = AbstractJsonable.fromJson(Config.class, readJson(file));

            if (config.data.length() == 0)
                config.data = getEtc("data").getAbsolutePath();

            if (config.bulkSize < 128) config.bulkSize = 128;
            if (config.flushBulkTime < 200) config.flushBulkTime = 200;

        } catch (Exception e) {
            config = new Config();
        }

        File f = new File(config.data);
        f.mkdirs();

        Setting.config = config;
        return config;
    }

    public static River getRiver() throws IOException
    {
        return getRiver(new File(Setting.ETC_PATH, RIVER_FILE));
    }

    public static River getRiver(File file) throws IOException
    {
        if (Setting.river != null)
            return Setting.river;

        River river = AbstractJsonable.fromJson(River.class, readJson(file));
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
        Setting.river = river;
        return river;
    }

    public static BinLog getBinLog()
    {
        Config config = getConfig();
        return getBinLog(new File(config.data, BINLOG_FILE));
    }

    public static BinLog getBinLog(File file)
    {
        if (Setting.binLog != null)
            return Setting.binLog;

        BinLog log;
        try {
             log = AbstractJsonable.fromJson(BinLog.class, readJson(file));
        } catch (Exception e) {
            log = new BinLog();
        }

        Setting.binLog = log;
        return log;
    }

    public static void updateBinLog(String name, long pos) throws IOException
    {
        Config config = getConfig();
        updateBinLog(new File(config.data, BINLOG_FILE), name, pos);
    }

    public static void updateBinLog(File file, String name, long pos) throws IOException
    {
        if (!file.exists())
            file.createNewFile();

        binLog.name = name;
        binLog.position = pos;

        BufferedSink sink = Okio.buffer(Okio.sink(file));
        binLog.toJson(sink);
    }

    private static String replaceDBValue(String str, String db, String table)
    {
        return str.replace("${DB}", db).replace("${TABLE}", table);
    }
}
