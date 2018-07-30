package com.fly.sync.dumper;

import com.fly.sync.contract.DatabaseListener;
import com.fly.sync.exception.DumpException;
import com.fly.sync.setting.BinLog;
import com.fly.sync.setting.Config;
import com.fly.sync.setting.River;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Dumper {

    protected Config config;
    protected River river;

    public final static Logger logger = LoggerFactory.getLogger(Dumper.class);

    public Dumper(Config config, River river)
    {
        this.config = config;
        this.river = river;
    }

    public synchronized void run(River.Database database, DatabaseListener listener) throws IOException, InterruptedException
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
            .append(database);

        for (String name: database.tables.keySet()
             ) {
            cmd.append(" ");
            cmd.append(name);
            if (null != listener) listener.onCreateTable(database, name);
        }

        System.out.println(cmd);


        Process process = Runtime.getRuntime().exec(cmd.toString());
        logger.info("Dump database [{}] from mysqldump.", database.db);
        dataThread(database, listener, process);
        errorThread(database, listener, process);

        process.waitFor();

    }

    public void dataThread(final River.Database database, final DatabaseListener listener, final Process process)
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
                            triggerMaster(listener, database, sql);
                        } else if (sql.startsWith("INSERT INTO")){
                            triggerInsert(listener, database, sql);
                        } else {
                            logger.info("Skip SQL {} ", sql);
                        }

                    }
                } catch (Exception e)
                {
                    process.destroy();
                    if (null != listener) listener.onError(database, e);
                }

            }
        });
    }

    public void errorThread(final River.Database database, final DatabaseListener listener, final Process process){
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

                        throw new DumpException(s);
                    }
                } catch (Exception e)
                {
                    process.destroy();
                    if (null != listener) listener.onError(database, e);
                }
            }
        });
    }

    private void triggerMaster(DatabaseListener listener, River.Database database, String sql) {
        Pattern pattern = Pattern.compile("^\\s?+CHANGE\\s?+MASTER\\s?+TO\\s?+MASTER_LOG_FILE\\s?+=\\s?+['\"]([^'\"]*?)['\"],\\s?+MASTER_LOG_POS\\s?+=\\s?+([\\d]*?);", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            BinLog.Position position = new BinLog.Position();
            position.name = matcher.group(1);
            position.position = Long.parseLong(matcher.group(2));

            if (null != listener) listener.onPostionChange(database, position);
        }
    }

    private void triggerInsert(DatabaseListener listener, River.Database database, String sql)
    {

    }


}
