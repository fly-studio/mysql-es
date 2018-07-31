package com.fly.sync.mysql;

import com.fly.sync.contract.DatabaseListener;
import com.fly.sync.exception.FatalDumpException;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.Config;
import com.fly.sync.setting.River;
import com.sun.istack.internal.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Dumper {

    private Config config;
    private River river;
    private River.Database database;
    private DatabaseListener listener;
    private BinLog.Position position = new BinLog.Position();

    public final static Logger logger = LoggerFactory.getLogger(Dumper.class);

    public Dumper(@NotNull Config config, @NotNull River river, @NotNull River.Database database, @NotNull DatabaseListener listener)
    {
        this.config = config;
        this.river = river;
        this.database = database;
        this.listener = listener;
    }

    public void run()
    {
        StringBuilder cmd = new StringBuilder();

        /*
         * /usr/bin/mysqldump --host=XXXX --port=3306 --user=root --password=x xxxxxxxxxxxx \
         * --master-data --single-transaction --skip-lock-tables --compact --skip-opt \
         * --quick --no-create-info --skip-extended-insert --set-gtid-purged=OFF --default-character-set=utf8 \
         * db table1 table2 table3 table4
         */
        cmd.append(config.mysqldump)
            .append(" --host=")
            .append(river.my.host)
            .append(" --port=")
            .append(river.my.port)
            .append(" --user=")
            .append(river.my.user)
            .append(" --password=")
            .append(river.my.password)
            .append(" --default-character-set=")
            .append(river.charset)
            .append(" --master-data --single-transaction --skip-lock-tables --compact --skip-opt --quick --no-create-info --skip-extended-insert --set-gtid-purged=OFF ")
            .append(database.db);

        for (String name: database.tables.keySet()
             ) {
            cmd.append(" ");
            cmd.append(name);
            listener.onCreateTable(database, name);
        }

        logger.info(cmd.toString().replace(river.my.password, "*"));

        try {
            Process process = Runtime.getRuntime().exec(cmd.toString());
            logger.info("Dump database [{}] from mysqldump.", database.db);
            errorThread(process);
            dataThread(process);
            process.waitFor();
            process.destroy();
        } catch (IOException | InterruptedException e)
        {
            listener.onError(database, new FatalDumpException(e));
        }

    }

    private void dataThread(final Process process)
    {
        new Thread(new Runnable() {
            @Override
            public void run() {
                String sql;
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

                try {
                    while((sql = bufferedReader.readLine()) != null)
                    {
                        if (sql.startsWith("CHANGE MASTER TO MASTER_LOG_FILE"))
                        {
                            triggerMaster(sql);
                        } else if (sql.startsWith("INSERT INTO")){
                            triggerInsert(sql);
                        } else {
                            logger.info("Skip SQL {} ", sql);
                        }
                    }

                } catch (IOException e)
                {
                    process.destroy();
                    position.reset();
                    listener.onError(database, new FatalDumpException(e));
                } finally {
                    try {
                        bufferedReader.close();
                    } catch (Exception e) {}
                }
                logger.info("Dump database: \"{}\" complete;", database.db);

                // change the binlog after insert
                if (!position.isEmpty())
                    listener.onPositionChange(database, position);
            }
        }).start();
    }

    private void errorThread(final Process process){
        new Thread(new Runnable() {
            @Override
            public void run() {
                String s;
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

                try {
                    while((s = bufferedReader.readLine()) != null)
                    {
                        if (s.contains("[Warning]"))
                            continue;

                        process.destroy();
                        position.reset();
                        listener.onError(database, new FatalDumpException(s));
                        break;
                    }
                } catch (IOException e)
                {
                    process.destroy();
                    position.reset();
                    listener.onError(database, new FatalDumpException(e));
                } finally {
                    try {
                        bufferedReader.close();
                    } catch (Exception e) {}
                }
            }
        }).start();
    }

    private void triggerMaster(String sql) {
        synchronized (Dumper.class)
        {
            Pattern pattern = Pattern.compile("^\\s?+CHANGE\\s?+MASTER\\s?+TO\\s?+MASTER_LOG_FILE\\s?+=\\s?+['\"]([^'\"]*?)['\"],\\s?+MASTER_LOG_POS\\s?+=\\s?+([\\d]*?);", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
            Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                position.name = matcher.group(1);
                position.position = Long.parseLong(matcher.group(2));
            }
        }
    }

    private void triggerInsert(String sql)
    {
        listener.onInsert(database, sql);
    }

}
