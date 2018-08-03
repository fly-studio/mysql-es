package com.fly.sync.mysql.parser;

import com.fly.sync.setting.BinLog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PositionParser {

    public static BinLog.Position parse(String sql)
    {
        Pattern pattern = Pattern.compile("^\\s?+CHANGE\\s?+MASTER\\s?+TO\\s?+MASTER_LOG_FILE\\s?+=\\s?+['\"]([^'\"]*?)['\"],\\s?+MASTER_LOG_POS\\s?+=\\s?+([\\d]*?);", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        return matcher.find() ? new BinLog.Position(matcher.group(1), Long.parseLong(matcher.group(2))) : null;
    }
}
