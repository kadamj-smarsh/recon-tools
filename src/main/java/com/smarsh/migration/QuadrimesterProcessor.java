package com.smarsh.migration;

import com.smarsh.athena.AthenaQueryRunner;
import com.smarsh.backlogger.Logger;
import com.smarsh.backlogger.RetryExecutor;
import com.smarsh.migration.QuadrimesterMonths.MonthWindow;
import com.smarsh.migration.QuadrimesterMonths.QuadKey;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The per-quadrimester pipeline: 4 monthly dedup UNLOADs (bounded
 * concurrency) -> MSCK REPAIR -> verify counts against stage2 -> on
 * pass, final UNLOAD; on mismatch, report and leave unmarked for retry.
 */
public class QuadrimesterProcessor {

    private final AthenaQueryRunner runner;
    private final MigrationConfig config;
    private final Logger logger;
    private final MismatchReporter mismatchReporter;
    private final String tableName;
    private final String tempPath;

    QuadrimesterProcessor(AthenaQueryRunner runner, MigrationConfig config, Logger logger,
                            MismatchReporter mismatchReporter) {
        this.runner = runner;
        this.config = config;
        this.logger = logger;
        this.mismatchReporter = mismatchReporter;
        this.tableName = MigrationSql.tempTableName(config.reportingEntity);
        this.tempPath = MigrationSql.tempPath(config.bucket, config.reportingEntity);
    }

    /** Returns true if this quadrimester completed successfully (final UNLOAD ran). */
    boolean process(QuadKey key) throws Exception {
        List<MonthWindow> months = QuadrimesterMonths.monthsOf(key);

        // Step 1: 4 monthly dedup UNLOADs, bounded concurrency, independent of each other.
        AtomicReference<Exception> firstFailure = new AtomicReference<>();
        Semaphore limiter = new Semaphore(config.concurrency);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (MonthWindow mw : months) {
                limiter.acquire();
                executor.submit(() -> {
                    try {
                        String sql = MigrationSql.monthlyUnload(config.stage2Table, config.reportingEntity, key, mw, tempPath);
                        RetryExecutor.withRetry(() -> runner.runQuery(sql), "unload-" + key.label() + "-" + mw.monthLabel(), logger);
                    } catch (Exception e) {
                        firstFailure.compareAndSet(null, e);
                    } finally {
                        limiter.release();
                    }
                });
            }
            // try-with-resources blocks here until all 4 finish.
        }
        if (firstFailure.get() != null) {
            logger.warn("Quadrimester [" + key.label() + "] aborted: a monthly UNLOAD failed after retries: "
                + firstFailure.get().getMessage());
            return false;
        }

        // Step 2: discover the new partitions.
        RetryExecutor.withRetry(() -> runner.runQuery(MigrationSql.repairTable(tableName)), "repair-" + key.label(), logger);

        // Step 3: verify.
        List<List<String>> tempRows = RetryExecutor.withRetry(
            () -> runner.runQuery(MigrationSql.verifyTempCounts(tableName, key)), "verify-temp-" + key.label(), logger);
        List<List<String>> stageRows = RetryExecutor.withRetry(
            () -> runner.runQuery(MigrationSql.verifyStageCounts(config.stage2Table, config.reportingEntity, key)),
            "verify-stage-" + key.label(), logger);

        long tempTotal = parseLong(tempRows, 0);
        long tempDistinct = parseLong(tempRows, 1);
        long stageDistinct = parseLong(stageRows, 1);

        if (tempTotal != tempDistinct) {
            logger.warn("Quadrimester [" + key.label() + "] self-consistency note: temp table total_rows ("
                + tempTotal + ") != distinct_sources (" + tempDistinct + ") - dedup may not have fully deduplicated.");
        }

        if (tempDistinct != stageDistinct) {
            logger.warn(String.format(
                "*** MISMATCH for quadrimester [%s]: temp distinct_sources=%d vs stage2 distinct_sources=%d (delta=%d). "
                    + "Final UNLOAD skipped, not marked complete - will retry on a future run. ***",
                key.label(), tempDistinct, stageDistinct, tempDistinct - stageDistinct));
            mismatchReporter.report(key.label(), config.reportingEntity, tempDistinct, tempTotal, stageDistinct);
            return false;
        }

        // Step 4: verified - final UNLOAD to the permanent destination.
        String finalPath = MigrationSql.finalPath(config.bucket, config.reportingEntity, key);
        String finalSql = MigrationSql.finalUnload(tableName, key, finalPath);
        RetryExecutor.withRetry(() -> runner.runQuery(finalSql), "final-unload-" + key.label(), logger);

        logger.info("Quadrimester [" + key.label() + "] verified (distinct_sources=" + tempDistinct
            + ") and unloaded to " + finalPath);
        return true;
    }

    private static long parseLong(List<List<String>> rows, int column) {
        if (rows.isEmpty()) return 0;
        return Long.parseLong(rows.get(0).get(column));
    }
}
