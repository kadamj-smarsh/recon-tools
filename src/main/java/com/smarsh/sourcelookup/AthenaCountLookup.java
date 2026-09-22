package com.smarsh.sourcelookup;

import com.smarsh.athena.AthenaQueryRunner;

/**
 * Runs the narrowed COUNT(*) confirmation query per (sourceId, quadrimester),
 * e.g.:
 *   SELECT COUNT(*) FROM {table}
 *   WHERE reporting_entity='{entity}' AND quadrimester='{quad}' AND source_id='{sourceId}'
 *
 * Filtering by quadrimester (derived from the ES key's own date, see
 * Quadrimester.of) is what keeps this cheap - the given un-narrowed
 * example query (source_id alone, no quadrimester) scans the whole table
 * and "gets exhausted" per the original report.
 */
public class AthenaCountLookup {

    private final AthenaQueryRunner runner;
    private final String table;
    private final String reportingEntity;

    AthenaCountLookup(AthenaQueryRunner runner, String table, String reportingEntity) {
        this.runner = runner;
        this.table = table;
        this.reportingEntity = reportingEntity;
    }

    long count(String sourceId, String quadrimester) throws Exception {
        String sql = "SELECT COUNT(*) AS cnt FROM " + table + " "
            + "WHERE reporting_entity = '" + escape(reportingEntity) + "' "
            + "AND quadrimester = '" + escape(quadrimester) + "' "
            + "AND source_id = '" + escape(sourceId) + "'";

        var rows = runWithRetry(sql);
        if (rows.isEmpty()) return 0;
        return Long.parseLong(rows.get(0).get(0));
    }

    private static final int MAX_ATTEMPTS = 3;
    private static final long[] BACKOFF_MS = {2000, 5000};

    private java.util.List<java.util.List<String>> runWithRetry(String sql) throws Exception {
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
