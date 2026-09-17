package com.smarsh.reconcile;

import com.smarsh.athena.AthenaQueryRunner;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds and runs the Athena side of each drill-down level, using the
 * shared AthenaQueryRunner (com.smarsh.athena) as-is - it's already
 * generic over arbitrary SQL strings.
 */
public class AthenaLevelQueries {

    private final AthenaQueryRunner runner;
    private final String table;
    private final String reportingEntity;

    AthenaLevelQueries(AthenaQueryRunner runner, String table, String reportingEntity) {
        this.runner = runner;
        this.table = table;
        this.reportingEntity = reportingEntity;
    }

    /** Month level: whole year, all 3 quadrimesters. Key = "yyyy-MM". */
    Map<String, Long> monthCounts(int year) throws Exception {
        String quadIn = "'" + year + "-T1','" + year + "-T2','" + year + "-T3'";
        String sql = "SELECT DATE_FORMAT(FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000),'%Y-%m') AS year_month, "
            + "COUNT(DISTINCT key) AS key_count "
            + "FROM " + table + " "
            + "WHERE reporting_entity = '" + escape(reportingEntity) + "' "
            + "AND quadrimester IN (" + quadIn + ") "
            + "GROUP BY DATE_FORMAT(FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000),'%Y-%m') "
            + "ORDER BY year_month";

        Map<String, Long> result = new LinkedHashMap<>();
        for (List<String> row : runWithRetry(sql)) {
            result.put(row.get(0), Long.parseLong(row.get(1)));
        }
        return result;
    }

    /** Day/hour/minute/second level: single quadrimester, one explicit window, GROUP BY {timeFunction}. Key = int. */
    Map<Integer, Long> unitCounts(String timeFunction, String quadrimester, TimeWindow window) throws Exception {
        String sql = "SELECT " + timeFunction + "(FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000)) AS unit, "
            + "COUNT(DISTINCT key) AS key_count "
            + "FROM " + table + " "
            + "WHERE reporting_entity = '" + escape(reportingEntity) + "' "
            + "AND quadrimester = '" + escape(quadrimester) + "' "
            + "AND FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000) >= TIMESTAMP '" + window.athenaStart() + "' "
            + "AND FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000) < TIMESTAMP '" + window.athenaEndExclusive() + "' "
            + "GROUP BY " + timeFunction + "(FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000)) "
            + "ORDER BY unit";

        Map<Integer, Long> result = new LinkedHashMap<>();
        for (List<String> row : runWithRetry(sql)) {
            result.put(Integer.parseInt(row.get(0)), Long.parseLong(row.get(1)));
        }
        return result;
    }

    /** Leaf level: every distinct key in the window (no LIMIT - the paginator already returns everything). */
    List<String> distinctKeys(String quadrimester, TimeWindow window) throws Exception {
        String sql = "SELECT DISTINCT key "
            + "FROM " + table + " "
            + "WHERE reporting_entity = '" + escape(reportingEntity) + "' "
            + "AND quadrimester = '" + escape(quadrimester) + "' "
            + "AND FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000) >= TIMESTAMP '" + window.athenaStart() + "' "
            + "AND FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000) < TIMESTAMP '" + window.athenaEndExclusive() + "'";

        return runWithRetry(sql).stream().map(row -> row.get(0)).toList();
    }

    private static final int MAX_ATTEMPTS = 3;
    private static final long[] BACKOFF_MS = {2000, 5000};

    private List<List<String>> runWithRetry(String sql) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return runner.runQuery(sql);
            } catch (Exception e) {
                last = e;
                System.err.printf("[Athena] attempt %d/%d failed: %s%n", attempt, MAX_ATTEMPTS, e.getMessage());
                if (attempt < MAX_ATTEMPTS) Thread.sleep(BACKOFF_MS[attempt - 1]);
            }
        }
        throw last;
    }

    private static String escape(String s) {
        return s.replace("'", "''");
    }
}
