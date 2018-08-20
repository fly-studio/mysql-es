package com.fly.sync.action;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.AbstractWriter;
import com.fly.sync.executor.Statistic;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportAction implements AbstractAction {
    public final static Logger logger = LoggerFactory.getLogger(ReportAction.class);

    public void execute(AbstractWriter writer) {
        Statistic statistic = writer.getDbFactory().getStatistic();

        long millis = System.currentTimeMillis()- statistic.getCreatedAt();
        String hms = DurationFormatUtils.formatDuration(millis, "d 'days' HH:mm:ss");

        logger.trace("\nExecute Information:\n" +
                        "+ Memory Usage: {}\n" +
                        "+ Duration: {}\n" +
                        "+ Records: {}\n" +
                        "\t- Dump: {}\n" +
                        "\t- Canal: {}\n" +
                        "\t+ DML:\n" +
                        "\t\t- Insert: {}\n" +
                        "\t\t- Update: {}\n" +
                        "\t\t- Delete: {}\n" +
                        "- Relate: {}",
                FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()),
                hms,
                statistic.getRecordCount(),
                statistic.getDumpCount(),
                statistic.getCanalCount(),
                statistic.getInsertCount(),
                statistic.getUpdateCount(),
                statistic.getDeleteCount(),
                statistic.getRelateCount()
        );
    }

    @Override
    public String getGroup() {
        return ReportAction.class.getName();
    }

}
