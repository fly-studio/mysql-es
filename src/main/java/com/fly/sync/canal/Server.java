package com.fly.sync.canal;

import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.instance.manager.model.CanalStatus;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.fly.core.database.SqlUtils;
import com.fly.sync.Main;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class Server {

    private CanalInstanceWithManager manager;
    private final static AtomicLong SERVER_ID = new AtomicLong(6586);
    private River.Database database;
    private Canal canal;

    public Server(River river, River.Database database, BinLog.Position position) {
        this.database = database;

        canal = Config.build(river, database, position);

        manager = new CanalInstanceWithManager(canal, getFilter());
    }

    public String getFilter()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(Pattern.quote(database.schemaName))
            .append("\\.");

        if (database.associates.size() > 1)
        {
            sb.append("(");
            StringJoiner sj = new StringJoiner("|");

            for (String tableName: database.associates.keySet()
            ) {
                sj.add(Pattern.quote(tableName));
            }
            sb.append(sj.toString())
                .append(")");
        } else {
            sb.append(database.associates.keySet().toArray()[0]);
        }

        return sb.toString();
    }

    public CanalInstanceWithManager getManager() {
        return manager;
    }

    public BinLog.Position getBinLogPosition()
    {
        CanalEventParser eventParser = manager.getEventParser();

        return eventParser instanceof AbstractEventParser ?  BinLog.Position.create(((AbstractEventParser)eventParser).getLogPositionManager().getLatestIndexBy(database.schemaName)) : null;
    }

    public River.Database getDatabase() {
        return database;
    }

    public Canal getCanal() {
        return canal;
    }

    public CanalParameter getCanalParameter()
    {
        return canal.getCanalParameter();
    }

    public void start()
    {

        manager.start();
    }

    public void stop()
    {
        manager.stop();
    }

    private static class Config {

        public static Canal build(River river, River.Database database, BinLog.Position position)
        {
            Canal canal = new Canal();

            canal.setId(SERVER_ID.incrementAndGet());
            canal.setGmtCreate(new Date());
            canal.setGmtModified(new Date());
            canal.setName(database.schemaName);
            canal.setDesc(Main.DESCRIPTION);
            canal.setStatus(CanalStatus.STOP);
            canal.setCanalParameter(buildCanalParameter(river, database, position));

            return canal;
        }

        private static CanalParameter buildCanalParameter(River river, River.Database database, BinLog.Position position)
        {
            CanalParameter parameter = new CanalParameter();

            parameter.setCanalId(SERVER_ID.incrementAndGet());

            parameter.setStorageMode(CanalParameter.StorageMode.MEMORY);
            // 以下参数后续版本实现
            parameter.setFileStorageDirectory(Setting.getDataDir().getAbsolutePath());
            parameter.setFileStorageStoreCount(1000);
            parameter.setFileStorageRollverCount(1000);
            parameter.setFileStoragePercentThresold(98);

            parameter.setMasterAddress(new InetSocketAddress(river.my.host, river.my.port));
            parameter.setMasterLogfileName(position.name);
            parameter.setMasterLogfileOffest(position.position);
            parameter.setMasterTimestamp(System.currentTimeMillis() / 1_000);
            parameter.setMasterUsername(river.my.user);
            parameter.setMasterPassword(river.my.password);

            parameter.setSlaveId(SERVER_ID.incrementAndGet());
            parameter.setConnectionCharset(river.charset.equalsIgnoreCase("utf8") ? "UTF-8" : river.charset);
            parameter.setDetectingSQL(SqlUtils.HEARTBEAT);
            parameter.setDefaultDatabaseName(database.schemaName);


            parameter.setIndexMode(CanalParameter.IndexMode.MEMORY);
            parameter.setDdlIsolation(true);

            return parameter;
        }
    }
}
