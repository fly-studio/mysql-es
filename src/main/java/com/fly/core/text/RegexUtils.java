package com.fly.core.text;

public class RegexUtils {
    /**
     * Java implementation of preg_quote
     * {@link http://phpjs.org/functions/preg_quote/}
     * @param pStr - string to be REGEX quoted
     * @return quoted string
     */
    public static String preg_quote(String pStr) {
        return pStr.replaceAll("[.\\\\+*?\\[\\^\\]$(){}=!<>|:\\-]", "\\\\$0");
    }
}
