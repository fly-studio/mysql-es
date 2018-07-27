package com.fly.sync.setting;

import com.fly.core.io.Io;
import com.fly.core.text.json.Jsonable;
import com.fly.sync.exception.ConfigException;
import com.sun.istack.internal.NotNull;
import okio.BufferedSink;
import okio.Okio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class Setting {

    public static String ETC_PATH = null;

    static {
        String etcPath = System.getProperty("es.etc.path");
        if (etcPath != null && etcPath.length() != 0)
            ETC_PATH = etcPath;
        else
            ETC_PATH = new File(System.getProperty("user.dir"), "etc").getAbsolutePath();

    }

    public final static String RIVER_FILE = "river.json";
    public final static String CONFIG_FILE = "config.json";
    public final static String BINLOG_FILE = "binlog.json";

    public final static Logger logger = LoggerFactory.getLogger(Setting.class);
    public static Config config;
    public static River river;
    public static BinLog binLog;

    public static File getEtcPath()
    {
        return new File(ETC_PATH);
    }

    public static File getEtcPath(@NotNull String filename)
    {
        return new File(getEtcPath(), filename);
    }

    public static File getEtcPath(@NotNull File file)
    {
        return file.isAbsolute() ? file : getEtcPath(file.getPath());
    }

    public static Config getConfig() throws Exception
    {
        return getConfig(getEtcPath(CONFIG_FILE));
    }

    public static Config getConfig(File file) throws Exception
    {
        if (Setting.config != null)
            return Setting.config;

        Config config;

        try {
            config = Jsonable.fromJson(Config.class, Io.readJson(file));
        } catch (Exception e) {
            throw new ConfigException(e.getMessage(), e);
        }

        if (config.logDir == null) {
            config.logDir = getEtcPath("log");
            logger.warn("Invaid [log_dir] in \""+ CONFIG_FILE +"\", path to default.");
        }

        config.logDir = getEtcPath(config.logDir);
        System.setProperty("es.log.path", config.logDir.getAbsolutePath());

        // locate to user's log4j2.xml
        LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(getEtcPath("log4j2.xml").toURI());

        if (config.dataDir == null)
        {
            config.dataDir = getEtcPath("data");
            logger.warn("Invaid [data_dir] in \"{}\", path to default.", CONFIG_FILE);
        }

        config.dataDir = getEtcPath(config.dataDir);
        System.setProperty("es.data.path", config.dataDir.getAbsolutePath());


        if (config.bulkSize < 10)
        {
            config.bulkSize = 10;
            logger.warn("[bulk_size] in \"{}\" less than 10, auto change to 10.", CONFIG_FILE);
        }

        if (config.flushBulkTime < 200)
        {
            config.flushBulkTime = 200;
            logger.warn("[flush_bulk_time] in \"{}\" is less than 200, auto change to 200.", CONFIG_FILE);
        }

        logger.info("The [data] directory locate to {}", config.dataDir.getAbsolutePath());
        logger.info("The [log] directory locate to {}", config.logDir.getAbsolutePath());

        try {
            config.dataDir.mkdirs();
            config.logDir.mkdirs();
        } catch (Exception e)
        {
            throw new ConfigException(e.getMessage(), e);
        }

        Setting.config = config;

        logger.error("Loaded config.json.");
        return config;
    }

    public static River getRiver() throws IOException
    {
        return getRiver(getEtcPath(RIVER_FILE));
    }

    public static River getRiver(File file) throws IOException
    {
        if (Setting.river != null)
            return Setting.river;

        River river = Jsonable.fromJson(River.class, Io.readJson(file));
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
        return getBinLog(new File(config.dataDir, BINLOG_FILE));
    }

    public static BinLog getBinLog(File file)
    {
        if (Setting.binLog != null)
            return Setting.binLog;

        BinLog log;
        try {
             log = Jsonable.fromJson(BinLog.class, Io.readJson(file));
        } catch (Exception e) {
            log = new BinLog();
        }

        Setting.binLog = log;
        return log;
    }

    public static void updateBinLog(String name, long pos) throws IOException
    {
        updateBinLog(new File(config.dataDir, BINLOG_FILE), name, pos);
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

    public static void readSettings() throws Exception
    {
        getConfig();
        getRiver();
        getBinLog();
    }
}
