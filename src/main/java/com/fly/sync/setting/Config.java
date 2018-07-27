package com.fly.sync.setting;

import com.fly.core.text.json.Jsonable;
import com.squareup.moshi.Json;

import java.io.File;

public class Config extends Jsonable {

    @Json(name = "data_dir") public File dataDir;
    @Json(name = "log_dir") public File logDir;
    public String mysqldump = "mysqldump";
    @Json(name = "bulk_size") public int bulkSize = 128;
    @Json(name = "flush_bulk_time") public int flushBulkTime = 200;


}
