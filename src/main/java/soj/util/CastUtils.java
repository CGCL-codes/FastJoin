package soj.util;

import java.util.List;

public class CastUtils
{
    public static String getString(Object o) {
        if (o == null) {
            return null;
        }
        else {
            return o.toString();
        }
    }

    public static String getString(List<Object> values, int i) {
        return getString(values.get(i));
    }

    public static int getInt(Object o) {
        if (o instanceof Long) {
            return ((Long) o).intValue();
        }
        else if (o instanceof Integer) {
            return ((Integer) o).intValue();
        }
        else if (o instanceof Short) {
            return ((Short) o).intValue();
        }
        else if (o instanceof String) {
            return Integer.parseInt((String) o);
        }
        else {
            throw new IllegalArgumentException("Failed to convert " + o
                    + " to int");
        }
    }

    public static int getInt(List<Object> values, int i) {
        return getInt(values.get(i));
    }

    public static long getLong(Object o) {
        if (o instanceof Long) {
            return ((Long) o).longValue();
        }
        else if (o instanceof Integer) {
            return ((Integer) o).longValue();
        }
        else if (o instanceof Short) {
            return ((Short) o).longValue();
        }
        else if (o instanceof String) {
            return Long.parseLong((String) o);
        }
        else {
            throw new IllegalArgumentException("Failed to convert " + o
                    + " to long");
        }
    }

    public static long getLong(List<Object> values, int i) {
        return getLong(values.get(i));
    }

    public static boolean getBoolean(Object o) {
        if (o instanceof Boolean) {
            return ((Boolean) o).booleanValue();
        }
        else if (o instanceof String) {
            return Boolean.parseBoolean((String) o);
        }
        else {
            throw new IllegalArgumentException("Failed to convert " + o
                    + " to boolean");
        }
    }

    public static boolean getBoolean(List<Object> values, int i) {
        return getBoolean(values.get(i));
    }

    public static List getList(Object o) {
        if (o instanceof List) {
            return (List) o;
        }
        else {
            throw new IllegalArgumentException("Failed to convert " + o
                    + " to list of objects");
        }
    }
}