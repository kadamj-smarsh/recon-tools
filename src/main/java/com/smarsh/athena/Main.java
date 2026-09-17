package com.smarsh.athena;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * For each year bucket (see YearBuckets), runs a per-year, quadrimester-
 * filtered Athena query counting DISTINCT keys, and appends the resulting
 * (year, key_count) row(s) to a local CSV. A small, bounded batch (~30
 * queries for the full range) - a single retry on transient failure per
 * bucket, plus a small resume manifest (see PROCESSED_SUFFIX) so a rerun
 * skips buckets already completed.
 *
 * Note: the output CSV alone can't tell us which BUCKETS are done - the
 * special multi-year buckets (pre-1970, 1970-1999, post-2026) write rows
 * keyed by the actual data year (e.g. "1975"), not the bucket label, and a
 * bucket with genuinely zero matching data writes no rows at all but is
 * still "done." So completed bucket labels are tracked in a separate
 * manifest file instead of trying to infer them from the CSV's contents.
 *
 * Run: java -jar target/athena-year-key-count-1.0.0.jar
 *        --database <db> --reporting-entity <entity> [options]
 */
public class Main {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final Object CSV_LOCK = new Object();
    private static final Object MANIFEST_LOCK = new Object();
    private static final String PROCESSED_SUFFIX = ".processed-buckets.txt";

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            run(CliConfig.parse(args));
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

    private static void run(CliConfig config) throws Exception {
        log("Database         : " + config.database);
        log("Table            : " + config.table);
        log("Reporting entity : " + config.reportingEntity);
        log("Region           : " + config.region);
        log("Workgroup        : " + config.workgroup);
        log("Output CSV       : " + config.outputCsv);
        log("Concurrency      : " + config.concurrency);
        log("S3 output loc.   : " + (config.s3OutputLocation != null ? config.s3OutputLocation
            : "(none - relying on workgroup default)"));
        Path manifestPath = Path.of(config.outputCsv.toString() + PROCESSED_SUFFIX);
        Set<String> alreadyProcessed = loadProcessedBuckets(manifestPath);
        List<YearBuckets.Bucket> pending = config.buckets.stream()
            .filter(b -> !alreadyProcessed.contains(b.label()))
            .toList();

        if (!alreadyProcessed.isEmpty()) {
            log("Resuming: " + alreadyProcessed.size()
                + " bucket(s) already completed in a prior run will be skipped: " + alreadyProcessed);
        }
        log("Year buckets     : " + pending.size() + " to run (of " + config.buckets.size() + " total) -> "
            + pending.stream().map(YearBuckets.Bucket::label).toList());

        if (pending.isEmpty()) {
            log("Nothing to do - every requested bucket is already in " + manifestPath);
            return;
        }

        boolean writeHeader = Files.notExists(config.outputCsv);
        try (PrintWriter csv = new PrintWriter(Files.newBufferedWriter(config.outputCsv, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
            if (writeHeader) {
                csv.println("year,key_count");
                csv.flush();
            }

            AthenaClient client = AthenaClient.builder().region(Region.of(config.region)).build();
            AthenaQueryRunner runner = new AthenaQueryRunner(client, config.database, config.workgroup, config.s3OutputLocation);

            Semaphore concurrencyLimiter = new Semaphore(config.concurrency);
            AtomicLong completed = new AtomicLong();
            AtomicLong failed = new AtomicLong();
            int total = pending.size();

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (YearBuckets.Bucket bucket : pending) {
                    concurrencyLimiter.acquire();
                    executor.submit(() -> {
                        try {
                            runBucketWithRetry(runner, config, bucket, csv);
                            markProcessed(manifestPath, bucket.label());
                        } catch (Exception e) {
                            failed.incrementAndGet();
                            log("FAILED bucket [" + bucket.label() + "] after retry: " + e.getMessage());
                        } finally {
                            concurrencyLimiter.release();
                            long done = completed.incrementAndGet();
                            log(String.format("Progress: %d/%d buckets done (%d failed)", done, total, failed.get()));
                        }
                    });
                }
                // try-with-resources blocks here until every submitted task finishes.
            }

            log("Done. " + (total - failed.get()) + "/" + total + " buckets succeeded this run.");
        }
    }

    private static void runBucketWithRetry(AthenaQueryRunner runner, CliConfig config,
                                             YearBuckets.Bucket bucket, PrintWriter csv) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= 2; attempt++) {
            try {
                String sql = buildSql(config.table, config.reportingEntity, bucket.quadrimesterValues());
                List<List<String>> rows = runner.runQuery(sql);
                writeRows(csv, bucket.label(), rows);
                return;
            } catch (Exception e) {
                last = e;
                log("[" + bucket.label() + "] attempt " + attempt + "/2 failed: " + e.getMessage());
                if (attempt < 2) Thread.sleep(5000);
            }
        }
        throw last;
    }

    private static void writeRows(PrintWriter csv, String bucketLabel, List<List<String>> rows) {
        synchronized (CSV_LOCK) {
            if (rows.isEmpty()) {
                log("[" + bucketLabel + "] returned 0 rows (no matching data for this bucket).");
                return;
            }
            for (List<String> row : rows) {
                // row = [year, key_count]
                csv.println(String.join(",", row));
            }
            csv.flush();
        }
    }

    private static Set<String> loadProcessedBuckets(Path manifestPath) throws Exception {
        Set<String> labels = new HashSet<>();
        if (Files.notExists(manifestPath)) return labels;
        for (String line : Files.readAllLines(manifestPath, StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) labels.add(trimmed);
        }
        return labels;
    }

    private static void markProcessed(Path manifestPath, String bucketLabel) throws Exception {
        synchronized (MANIFEST_LOCK) {
            Files.writeString(manifestPath, bucketLabel + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private static String buildSql(String table, String reportingEntity, List<String> quadrimesterValues) {
        String quotedEntity = reportingEntity.replace("'", "''");
        String quotedList = quadrimesterValues.stream()
            .map(v -> "'" + v.replace("'", "''") + "'")
            .reduce((a, b) -> a + "," + b)
            .orElseThrow();

        return "SELECT YEAR(FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000)) AS year, "
            + "COUNT(DISTINCT key) AS key_count "
            + "FROM " + table + " "
            + "WHERE reporting_entity = '" + quotedEntity + "' "
            + "AND quadrimester IN (" + quotedList + ") "
            + "GROUP BY YEAR(FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000)) "
            + "ORDER BY year";
    }

    private static synchronized void log(String message) {
        System.out.println("[" + LocalDateTime.now().format(TS) + "] " + message);
    }
}
