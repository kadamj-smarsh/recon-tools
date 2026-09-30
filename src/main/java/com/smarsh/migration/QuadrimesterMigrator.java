package com.smarsh.migration;

import com.smarsh.athena.AthenaQueryRunner;
import com.smarsh.backlogger.Logger;
import com.smarsh.migration.QuadrimesterMonths.QuadKey;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * For each requested quadrimester (derived from --years, optionally
 * narrowed by --quadrimesters): runs the per-month dedup UNLOAD -> MSCK
 * REPAIR -> verify-against-stage2 -> final-UNLOAD pipeline
 * (QuadrimesterProcessor), sequentially across quadrimesters (Athena
 * account-level concurrency limits + this is a stateful multi-step
 * pipeline per quadrimester - only the 4 monthly UNLOADs within one
 * quadrimester get bounded concurrency, not quadrimesters themselves).
 *
 * A single persistent temp table per reporting entity (not one per
 * quadrimester) is created once, if it doesn't already exist, before the
 * loop - see MigrationSql for the partitioning scheme (year/quadrimester/month).
 *
 * Resumable: quadrimesters already marked completed in
 * <output-dir>/processed-quadrimesters.txt (same run, same
 * --reporting-entity) are skipped. A verification mismatch or any other
 * per-quadrimester failure is logged and does NOT stop the run - it
 * moves on to the next quadrimester.
 *
 * Run: java -cp target/backlogger-reprocess-1.0.0.jar
 *        com.smarsh.migration.QuadrimesterMigrator
 *        --reporting-entity <entity> --years <csv> --database <db> [options]
 */
public class QuadrimesterMigrator {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            run(MigrationConfig.parse(args));
        } catch (IllegalStateException | IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            System.out.printf("Total run time: %.1fs%n", (System.currentTimeMillis() - start) / 1000.0);
        }
    }

    private static void run(MigrationConfig config) throws Exception {
        try (Logger logger = new Logger(config.outputDir)) {
            logger.info("Reporting entity : " + config.reportingEntity);
            logger.info("Database         : " + config.database);
            logger.info("Stage2 table     : " + config.stage2Table);
            logger.info("Bucket           : " + config.bucket);
            logger.info("Region           : " + config.region);
            logger.info("Workgroup        : " + config.workgroup);
            logger.info("Output dir       : " + config.outputDir);
            logger.info("Concurrency      : " + config.concurrency + " (monthly UNLOADs per quadrimester)");
            logger.info("Quadrimesters requested: " + config.quadrimesters.stream().map(QuadKey::label).toList());

            Set<String> completed = MigrationLedger.loadCompleted(config.outputDir);
            List<QuadKey> pending = config.quadrimesters.stream()
                .filter(k -> !completed.contains(k.label()))
                .toList();

            if (!completed.isEmpty()) {
                logger.info("Resuming: " + completed.size()
                    + " quadrimester(s) already completed in a prior run will be skipped: " + completed);
            }
            logger.info("Pending: " + pending.size() + " quadrimester(s) -> "
                + pending.stream().map(QuadKey::label).toList());

            if (pending.isEmpty()) {
                logger.info("Nothing to do - every requested quadrimester is already in the ledger.");
                return;
            }

            AthenaClient athenaClient = AthenaClient.builder().region(Region.of(config.region)).build();
            AthenaQueryRunner runner = new AthenaQueryRunner(athenaClient, config.database, config.workgroup, config.s3OutputLocation);

            String tableName = MigrationSql.tempTableName(config.reportingEntity);
            String tempPath = MigrationSql.tempPath(config.bucket, config.reportingEntity);
            logger.info("Temp table       : " + tableName);
            logger.info("Temp path        : " + tempPath);
            com.smarsh.backlogger.RetryExecutor.withRetry(
                () -> runner.runQuery(MigrationSql.createTableIfNotExists(tableName, tempPath)),
                "create-table", logger);

            try (MismatchReporter mismatchReporter = new MismatchReporter(config.outputDir)) {
                QuadrimesterProcessor processor = new QuadrimesterProcessor(runner, config, logger, mismatchReporter);

                AtomicLong succeeded = new AtomicLong();
                AtomicLong mismatchedOrFailed = new AtomicLong();

                for (QuadKey key : pending) {
                    logger.info("=== Starting quadrimester " + key.label() + " ===");
                    try {
                        boolean success = processor.process(key);
                        if (success) {
                            MigrationLedger.markCompleted(config.outputDir, key.label());
                            logger.info(key.label() + " completed and marked in ledger.");
                            succeeded.incrementAndGet();
                        } else {
                            logger.warn(key.label() + " ended without completing (mismatch or aborted step) - "
                                + "not marked complete, will retry on next run.");
                            mismatchedOrFailed.incrementAndGet();
                        }
                    } catch (Exception e) {
                        logger.warn("FAILED quadrimester [" + key.label() + "] (unexpected error): " + e.getMessage());
                        mismatchedOrFailed.incrementAndGet();
                        // Deliberately continue to the next quadrimester rather than aborting the whole run.
                    }
                }

                logger.info(String.format("Done: %d succeeded, %d mismatched/failed (out of %d pending).",
                    succeeded.get(), mismatchedOrFailed.get(), pending.size()));
            }
        }
    }
}
