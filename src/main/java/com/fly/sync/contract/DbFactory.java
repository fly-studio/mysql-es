package com.fly.sync.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.sync.es.Es;
import com.fly.sync.executor.Statistic;
import com.fly.sync.mysql.MySql;
import com.fly.sync.setting.River;

public interface DbFactory {

    Es getEs();
    MySql getMySql();
    River.Database getRiverDatabase();
    Statistic getStatistic();
    ObjectMapper getJsonMapper();

}
