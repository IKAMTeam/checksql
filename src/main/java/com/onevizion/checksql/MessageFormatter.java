package com.onevizion.checksql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessageFormatter {

    public static String format(String message, Object ... params) {
        Pattern regex = Pattern.compile("\\{\\}");
        Matcher regexMatcher = regex.matcher(message);
        int i = 0;
        StringBuffer sb = new StringBuffer();
        while (regexMatcher.find()) {
            if (params.length > i) {
                if (params[i] == null) {
                    params[i] = "null";
                }
                regexMatcher.appendReplacement(sb, Matcher.quoteReplacement(params[i].toString()));
            }
            else {
             regexMatcher.appendReplacement(sb, "");
            }
            i = i + 1;
        }
        regexMatcher.appendTail(sb);
        return sb.toString();
    }

}