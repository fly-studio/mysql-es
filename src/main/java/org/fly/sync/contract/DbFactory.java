package org.fly.sync.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.fly.sync.es.Es;
import org.fly.sync.executor.Statistic;
import org.fly.sync.mysql.MySql;
import org.fly.sync.setting.River;

public interface DbFactory {

    Es getEs();
    MySql getMySql();
    River.Database getRiverDatabase();
    Statistic getStatistic();
    ObjectMapper getJsonMapper();

}
