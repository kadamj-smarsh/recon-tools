package com.smarsh.reconcile;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * A half-open UTC time window [start, endExclusive) plus helpers to format
 * it for ES (gte/lte, inclusive both ends - lte = endExclusive minus 1ms)
 * and Athena ("TIMESTAMP '...'" literals, exclusive upper bound, matching
 * the given sample queries exactly).
 */
public record TimeWindow(ZonedDateTime start, ZonedDateTime endExclusive) {

    private static final DateTimeFormatter ES_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final DateTimeFormatter ATHENA_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static TimeWindow year(int year) {
        ZonedDateTime start = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        return new TimeWindow(start, start.plusYears(1));
    }

    static TimeWindow month(int year, int month) {
        ZonedDateTime start = ZonedDateTime.of(year, month, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        return new TimeWindow(start, start.plusMonths(1));
    }

    TimeWindow day(int day) {
        ZonedDateTime s = start.withDayOfMonth(day);
        return new TimeWindow(s, s.plusDays(1));
    }

    TimeWindow hour(int hour) {
        ZonedDateTime s = start.withHour(hour);
        return new TimeWindow(s, s.plusHours(1));
    }

    TimeWindow minute(int minute) {
        ZonedDateTime s = start.withMinute(minute);
        return new TimeWindow(s, s.plusMinutes(1));
    }

    TimeWindow second(int second) {
        ZonedDateTime s = start.withSecond(second);
        return new TimeWindow(s, s.plusSeconds(1));
    }

    String esGte() {
        return start.format(ES_FORMAT);
    }

    String esLte() {
        return endExclusive.minusNanos(1_000_000).format(ES_FORMAT);
    }

    String athenaStart() {
        return start.format(ATHENA_FORMAT);
    }

    String athenaEndExclusive() {
        return endExclusive.format(ATHENA_FORMAT);
    }

    /** T1=Jan-Apr, T2=May-Aug, T3=Sep-Dec. */
    static String quadrimesterOf(int year, int month) {
        int t = (month <= 4) ? 1 : (month <= 8) ? 2 : 3;
        return year + "-T" + t;
    }

    /** A short label for this window, used in log lines and the output CSV's path column. */
    String label(String levelName) {
        return levelName + "[" + start + " .. " + endExclusive + ")";
    }
}
