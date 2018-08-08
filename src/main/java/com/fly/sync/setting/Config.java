package com.fly.sync.setting;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fly.core.text.json.Jsonable;

import java.io.File;

public class Config extends Jsonable {

    @JsonProperty("data_dir") public File dataDir;
    @JsonProperty("log_dir") public File logDir;
    public String mysqldump = "mysqldump";
    @JsonProperty("bulk_size") public int bulkSize = 128;
    @JsonProperty("flush_bulk_time") public int flushBulkTime = 200;

}
