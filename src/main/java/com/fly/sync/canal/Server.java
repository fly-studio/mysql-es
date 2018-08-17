package com.fly.sync.canal;

import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.instance.manager.model.CanalStatus;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.fly.core.database.SqlUtils;
import com.fly.core.text.RegexUtils;
import com.fly.sync.Main;
import com.fly.sync.contract.AbstractLifeCycle;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLong;

public class Server extends AbstractLifeCycle {

    public final static Logger logger = LoggerFactory.getLogger(Server.class);
    private CanalInstanceWithManager canalInstance;
    private final static AtomicLong SERVER_ID = new AtomicLong(123);
    private River.Database database;
    private Canal canal;
    private String filter;

    public Server(River river, River.Database database, BinLog.Position position) {
        this.database = database;

        canal = Config.build(river, database, position);
        filter = buildFilter();

        canalInstance = new CanalInstanceWithManager(canal, filter);
    }

    public String buildFilter()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(RegexUtils.preg_quote(database.schemaName))
            .append("\\.");

        if (database.associates.size() > 1)
        {
            sb.append("(");
            StringJoiner sj = new StringJoiner("|");

            for (String tableName: database.associates.keySet()
            ) {
                sj.add(RegexUtils.preg_quote(tableName));
            }
            sb.append(sj.toString())
                .append(")");
        } else {
            sb.append(database.associates.keySet().toArray()[0]);
        }

        return sb.toString();
    }

    public String getFilter() {
        return filter;
    }

    public CanalInstanceWithManager getCanalInstance() {
        return canalInstance;
    }

    public BinLog.Position getBinLogPosition()
    {
        CanalEventParser eventParser = canalInstance.getEventParser();
        if (!(eventParser instanceof AbstractEventParser))
            return null;

        CanalLogPositionManager canalLogPositionManager = ((AbstractEventParser)eventParser).getLogPositionManager();
        LogPosition logPosition = canalLogPositionManager.getLatestIndexBy(database.schemaName);

        return logPosition != null ? BinLog.Position.create(logPosition) : null;
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

    @Override
    public void start()
    {
        super.start();

        canalInstance.start();
    }

    public void stop()
    {
        super.stop();

        canalInstance.stop();
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
