package com.fly.sync.action;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.executor.Statistic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ReportAction implements AbstractAction {
    public final static Logger logger = LoggerFactory.getLogger(ReportAction.class);

    @Override
    public void execute(DbFactory dbFactory) {
        Statistic statistic = dbFactory.getStatistic();

        long millis = System.currentTimeMillis() - statistic.getCreatedAt();
        String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
                TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
                TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));

        logger.trace("\nExecute Information:\n\tDuration: {}\n\tRecords: {}", hms, statistic.getRecordCount());
    }

}
