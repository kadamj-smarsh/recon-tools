package com.smarsh.sourcelookup;

/**
 * Derives the Athena "quadrimester" partition value from an ES key's own
 * date prefix - keys look like "YYYY/MM/DD/...". T1=Jan-Apr, T2=May-Aug,
 * T3=Sep-Dec (confirmed mapping, same as com.smarsh.reconcile.TimeWindow).
 */
public class Quadrimester {

    /** Returns e.g. "2010-T2", or null if the key doesn't parse as "YYYY/MM/...". */
    static String fromKey(String key) {
        if (key == null || key.length() < 7 || key.charAt(4) != '/' || key.charAt(7) != '/') return null;
        String yearStr = key.substring(0, 4);
        String monthStr = key.substring(5, 7);
        if (!isDigits(yearStr) || !isDigits(monthStr)) return null;

        int month = Integer.parseInt(monthStr);
        if (month < 1 || month > 12) return null;

        int t = (month <= 4) ? 1 : (month <= 8) ? 2 : 3;
        return yearStr + "-T" + t;
    }

    private static boolean isDigits(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) return false;
        }
        return true;
    }
}
