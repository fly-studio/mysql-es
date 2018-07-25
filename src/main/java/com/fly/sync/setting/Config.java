package com.fly.sync.setting;

import com.fly.core.contract.AbstractJsonable;
import com.squareup.moshi.Json;

public class Config extends AbstractJsonable {

    public String data = Setting.getEtc("data").getAbsolutePath();
    public String mysqldump = "mysqldump";
    @Json(name = "bulk_size") public int bulkSize = 128;
    @Json(name = "flush_bulk_time") public int flushBulkTime = 200;

}
