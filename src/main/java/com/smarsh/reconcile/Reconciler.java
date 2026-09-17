package com.smarsh.reconcile;

import java.io.PrintWriter;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The recursive drill-down: year -> month (all 3 quadrimesters) -> day
 * (single quadrimester) -> hour -> minute -> second, pruning any bucket
 * where ES and Athena already agree, and stopping early at whichever
 * level a mismatched bucket's ES count drops below 10000 (fetching actual
 * keys instead of drilling further). Sequential, not concurrent - this is
 * a diagnostic tool, not a bulk job, and real mismatches should be sparse
 * enough that the natural fan-out stays small.
 */
public class Reconciler {

    private static final int LEAF_THRESHOLD = 10000;
    private static final DateTimeFormatter ES_DAY_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final EsHistogramClient es;
    private final AthenaLevelQueries athena;
    private final PrintWriter outputCsv;
    private final Object csvLock = new Object();
    private final ReconcileStats stats;

    Reconciler(EsHistogramClient es, AthenaLevelQueries athena, PrintWriter outputCsv, ReconcileStats stats) {
        this.es = es;
        this.athena = athena;
        this.outputCsv = outputCsv;
        this.stats = stats;
    }

    void reconcileYear(int year) throws Exception {
        log("Year " + year + ": comparing monthly counts...");
        TimeWindow yearWindow = TimeWindow.year(year);
        String indexPattern = es.indexPatternFor(year);

        Map<String, Long> esMonthly = es.histogram(yearWindow, "month", "yyyy-MM", indexPattern);
        Map<String, Long> athenaMonthly = athena.monthCounts(year);

        for (String monthKey : union(esMonthly.keySet(), athenaMonthly.keySet())) {
            long esCount = esMonthly.getOrDefault(monthKey, 0L);
            long athenaCount = athenaMonthly.getOrDefault(monthKey, 0L);
            if (esCount == athenaCount) continue;

            int month = Integer.parseInt(monthKey.substring(5, 7));
            log("  Month " + monthKey + " differs: ES=" + esCount + " Athena=" + athenaCount);
            stats.mismatchedMonths.incrementAndGet();

            String quadrimester = TimeWindow.quadrimesterOf(year, month);
            TimeWindow monthWindow = TimeWindow.month(year, month);
            drillOrLeaf("month", monthWindow, esCount, quadrimester, indexPattern,
                () -> reconcileDays(year, month, quadrimester, monthWindow, indexPattern));
        }
    }

    private void reconcileDays(int year, int month, String quadrimester, TimeWindow monthWindow, String indexPattern) throws Exception {
        Map<String, Long> esDaily = es.histogram(monthWindow, "day", "yyyy-MM-dd", indexPattern);
        Map<Integer, Long> athenaDaily = athena.unitCounts("DAY", quadrimester, monthWindow);

        Set<Integer> esDayKeys = new LinkedHashSet<>();
        for (String k : esDaily.keySet()) esDayKeys.add(Integer.parseInt(k.substring(8, 10)));

        for (int day : union(esDayKeys, athenaDaily.keySet())) {
            long esCount = esDaily.getOrDefault(dayKeyString(monthWindow, day), 0L);
            long athenaCount = athenaDaily.getOrDefault(day, 0L);
            if (esCount == athenaCount) continue;

            log("    Day " + day + " differs: ES=" + esCount + " Athena=" + athenaCount);
            stats.mismatchedDays.incrementAndGet();

            TimeWindow dayWindow = monthWindow.day(day);
            drillOrLeaf("day", dayWindow, esCount, quadrimester, indexPattern,
                () -> reconcileHours(quadrimester, dayWindow, indexPattern));
        }
    }

    private void reconcileHours(String quadrimester, TimeWindow dayWindow, String indexPattern) throws Exception {
        Map<String, Long> esHourly = es.histogram(dayWindow, "hour", "yyyy-MM-dd'T'HH", indexPattern);
        Map<Integer, Long> athenaHourly = athena.unitCounts("HOUR", quadrimester, dayWindow);

        Set<Integer> esHourKeys = new LinkedHashSet<>();
        for (String k : esHourly.keySet()) esHourKeys.add(Integer.parseInt(k.substring(11, 13)));

        for (int hour : union(esHourKeys, athenaHourly.keySet())) {
            long esCount = firstMatching(esHourly, hourKeyPrefix(dayWindow, hour));
            long athenaCount = athenaHourly.getOrDefault(hour, 0L);
            if (esCount == athenaCount) continue;

            log("      Hour " + hour + " differs: ES=" + esCount + " Athena=" + athenaCount);
            stats.mismatchedHours.incrementAndGet();

            TimeWindow hourWindow = dayWindow.hour(hour);
            drillOrLeaf("hour", hourWindow, esCount, quadrimester, indexPattern,
                () -> reconcileMinutes(quadrimester, hourWindow, indexPattern));
        }
    }

    private void reconcileMinutes(String quadrimester, TimeWindow hourWindow, String indexPattern) throws Exception {
        Map<String, Long> esMinutely = es.histogram(hourWindow, "minute", "yyyy-MM-dd'T'HH:mm", indexPattern);
        Map<Integer, Long> athenaMinutely = athena.unitCounts("MINUTE", quadrimester, hourWindow);

        Set<Integer> esMinuteKeys = new LinkedHashSet<>();
        for (String k : esMinutely.keySet()) esMinuteKeys.add(Integer.parseInt(k.substring(14, 16)));

        for (int minute : union(esMinuteKeys, athenaMinutely.keySet())) {
            long esCount = firstMatching(esMinutely, minuteKeyPrefix(hourWindow, minute));
            long athenaCount = athenaMinutely.getOrDefault(minute, 0L);
            if (esCount == athenaCount) continue;

            log("        Minute " + minute + " differs: ES=" + esCount + " Athena=" + athenaCount);
            stats.mismatchedMinutes.incrementAndGet();

            TimeWindow minuteWindow = hourWindow.minute(minute);
            drillOrLeaf("minute", minuteWindow, esCount, quadrimester, indexPattern,
                () -> reconcileSeconds(quadrimester, minuteWindow, indexPattern));
        }
    }

    private void reconcileSeconds(String quadrimester, TimeWindow minuteWindow, String indexPattern) throws Exception {
        Map<String, Long> esSecondly = es.histogram(minuteWindow, "second", "yyyy-MM-dd'T'HH:mm:ss", indexPattern);
        Map<Integer, Long> athenaSecondly = athena.unitCounts("SECOND", quadrimester, minuteWindow);

        Set<Integer> esSecondKeys = new LinkedHashSet<>();
        for (String k : esSecondly.keySet()) esSecondKeys.add(Integer.parseInt(k.substring(17, 19)));

        for (int second : union(esSecondKeys, athenaSecondly.keySet())) {
            long esCount = firstMatching(esSecondly, secondKeyPrefix(minuteWindow, second));
            long athenaCount = athenaSecondly.getOrDefault(second, 0L);
            if (esCount == athenaCount) continue;

            log("          Second " + second + " differs: ES=" + esCount + " Athena=" + athenaCount);
            stats.mismatchedSeconds.incrementAndGet();

            // Deepest level - no finer granularity to drill into. Leaf-compare
            // regardless of esCount (paginate if it's still >= threshold).
            TimeWindow secondWindow = minuteWindow.second(second);
            leafCompare("second", secondWindow, esCount, quadrimester, indexPattern);
        }
    }

    /** Either leaf-compares (esCount < threshold) or drills one level deeper. */
    private void drillOrLeaf(String level, TimeWindow window, long esCount, String quadrimester,
                               String indexPattern, DrillAction drillDeeper) throws Exception {
        if (esCount < LEAF_THRESHOLD) {
            leafCompare(level, window, esCount, quadrimester, indexPattern);
        } else {
            drillDeeper.run();
        }
    }

    private void leafCompare(String level, TimeWindow window, long esCount, String quadrimester, String indexPattern) throws Exception {
        boolean paginate = esCount >= LEAF_THRESHOLD;
        log("    -> leaf compare at [" + level + "] " + window.athenaStart() + " (esCount=" + esCount
            + (paginate ? ", paginating" : "") + ")");

        List<String> esKeys = es.fetchKeys(window, indexPattern, paginate);
        List<String> athenaKeys = athena.distinctKeys(quadrimester, window);

        Set<String> esSet = new HashSet<>(esKeys);
        Set<String> athenaSet = new HashSet<>(athenaKeys);

        List<String> esOnly = esKeys.stream().filter(k -> !athenaSet.contains(k)).toList();
        List<String> athenaOnly = athenaKeys.stream().filter(k -> !esSet.contains(k)).toList();

        stats.esOnlyKeys.addAndGet(esOnly.size());
        stats.athenaOnlyKeys.addAndGet(athenaOnly.size());

        if (!esOnly.isEmpty() || !athenaOnly.isEmpty()) {
            writeOutstanding(level, window, "ES_ONLY", esOnly);
            writeOutstanding(level, window, "ATHENA_ONLY", athenaOnly);
            log("    -> " + esOnly.size() + " ES_ONLY, " + athenaOnly.size() + " ATHENA_ONLY keys written");
        }
    }

    private void writeOutstanding(String level, TimeWindow window, String source, List<String> keys) {
        if (keys.isEmpty()) return;
        synchronized (csvLock) {
            for (String key : keys) {
                outputCsv.printf("%s,%s,%s,%s,%s%n", level, window.athenaStart(), window.athenaEndExclusive(), source, key);
            }
            outputCsv.flush();
        }
    }

    // ── small helpers for matching ES's formatted bucket keys back to a specific numeric unit ──

    private static String dayKeyString(TimeWindow monthWindow, int day) {
        return monthWindow.day(day).start().format(ES_DAY_FMT);
    }

    private static String hourKeyPrefix(TimeWindow dayWindow, int hour) {
        return dayWindow.hour(hour).start().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH"));
    }

    private static String minuteKeyPrefix(TimeWindow hourWindow, int minute) {
        return hourWindow.minute(minute).start().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"));
    }

    private static String secondKeyPrefix(TimeWindow minuteWindow, int second) {
        return minuteWindow.second(second).start().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
    }

    private static long firstMatching(Map<String, Long> map, String exactKey) {
        return map.getOrDefault(exactKey, 0L);
    }

    private static <T extends Comparable<T>> List<T> union(Set<T> a, Set<T> b) {
        TreeSet<T> merged = new TreeSet<>(a);
        merged.addAll(b);
        return new ArrayList<>(merged);
    }

    private static void log(String message) {
        System.out.println(message);
    }

    @FunctionalInterface
    private interface DrillAction {
        void run() throws Exception;
    }
}
