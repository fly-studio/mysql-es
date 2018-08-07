package com.fly.sync.mysql.parser;

import org.apache.commons.lang.StringEscapeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertParser {

    private static final String INSERT_PATTERN = "^\\s?+INSERT INTO `(.+?)` VALUES \\((.+)\\);";

    public static String parseTable(String sql) {
        Pattern pattern = Pattern.compile(INSERT_PATTERN, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()){
            return matcher.group(1);
        }

        return null;
    }

    public static List<Object> parseValue(String sql)
    {
        Pattern pattern = Pattern.compile(INSERT_PATTERN, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        if (!matcher.find())
            return null;

        String str = matcher.group(2);

        int len = str.length();
        List<String> valueList = new ArrayList<>();

        int j = 0;
        boolean escaped = false;
        char ch;
        for (int i = 0; i < len ;) {
            ch = str.charAt(i);
            if (ch != '\'') // no string, read until comma ,
            {
                j = i + 1;
                for (; j < len && str.charAt(j) != ','; j++);

                valueList.add(str.substring(i, j));

                i = j + 1;
            } else { // read string until another single quote
                j = i + 1;

                escaped = false;

                for(; j < len;)
                {
                    ch = str.charAt(j);
                    if (ch == '\\')
                    {
                        // skip escaped character
                        j += 2;
                        escaped = true;
                    } else if (ch == '\'') {
                        break;
                    } else {
                        j++;
                    }
                }

                if (j >= len)
                    return null;

                String value = str.substring(i, j + 1);

                if (escaped) {
                    value = StringEscapeUtils.unescapeJava(value);
                }

                valueList.add(value);

                // skip ' and ,
                i = j + 2;
            }

            // skip blank
        }

        List<Object> list = new ArrayList<>();

        for (String value : valueList
             ) {
            if (value.equalsIgnoreCase("NULL"))
                list.add(null);
            else if (value.length() > 1 && value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'')
                list.add(value.substring(1, value.length() - 1));
            else
                list.add(value);
        }

        return list;
    }


}
