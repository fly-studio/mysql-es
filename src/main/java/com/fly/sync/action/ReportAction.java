package com.fly.sync.action;

import com.fly.sync.contract.AbstractAction;
import com.fly.sync.contract.DbFactory;
import com.fly.sync.executor.Statistic;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportAction implements AbstractAction {
    public final static Logger logger = LoggerFactory.getLogger(ReportAction.class);

    @Override
    public void execute(DbFactory dbFactory) {
        Statistic statistic = dbFactory.getStatistic();

        long millis = System.currentTimeMillis()- statistic.getCreatedAt();
        String hms = DurationFormatUtils.formatDuration(millis, "d 'days' HH:mm:ss");

        logger.trace("\nExecute Information:\n\t- Duration: {}\n\t- Records: {}\n\t\t- Dump: {}\n\t\t- Canal: {}", hms, statistic.getRecordCount(), statistic.getDumpCount(), statistic.getCanalCount());
    }

}
