package soj.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class TimeUtils
{
    private static final DateFormat defaultDateFormat = new SimpleDateFormat(
            "yyyyMMdd_HHmmss");

    public static String getTimestamp(Date date) {
        return defaultDateFormat.format(date);
    }

    public static String getTimestamp() {
        return getTimestamp(new Date());
    }

    public static String getTimestamp(long tsInMillis) {
        return getTimestamp(new Date(tsInMillis));
    }

    public static String getTimestamp(Date date, String format) {
        return (new SimpleDateFormat(format)).format(date);
    }

    public static String getTimestamp(String format) {
        return getTimestamp(new Date(), format);
    }

    public static String getTimestamp(long tsInMillis, String format) {
        return getTimestamp(new Date(tsInMillis), format);
    }
}