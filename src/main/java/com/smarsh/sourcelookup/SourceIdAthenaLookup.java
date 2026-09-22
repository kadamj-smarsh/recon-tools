package com.smarsh.sourcelookup;

import com.smarsh.athena.AthenaQueryRunner;
import com.smarsh.backlogger.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * For each sourceId in a CSV:
 *   1. Find its "key" in ES (EsKeyResolver) - a sourceId with no ES match
 *      is logged to unresolved-source-ids.csv and skipped from here on.
 *   2. Derive the Athena "quadrimester" partition value from that key's
 *      own YYYY/MM date (Quadrimester) - narrows the Athena confirmation
 *      query to one partition instead of scanning the whole table (which
 *      is what "gets exhausted" without it, per the original report).
 *   3. Run SELECT COUNT(*) ... quadrimester='...' AND source_id='...'
 *      (AthenaCountLookup) and write source_id,key,quadrimester,athena_count.
 *
 * Resumable (same ledger pattern as the other tools): source_ids already
 * in resolved-source-id-keys.csv or unresolved-source-ids.csv are skipped
 * on the next run against the same --output-dir.
 *
 * Run: java -cp target/backlogger-reprocess-1.0.0.jar
 *        com.smarsh.sourcelookup.SourceIdAthenaLookup
 *        --input source_ids.csv --reporting-entity <entity> --database <db> [options]
 */
public class SourceIdAthenaLookup {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            run(LookupConfig.parse(args));
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

    private static void run(LookupConfig config) throws Exception {
        try (Logger logger = new Logger(config.outputDir)) {
            logger.info("Input file       : " + config.inputFile);
            logger.info("Reporting entity : " + config.reportingEntity);
            logger.info("Database         : " + config.database);
            logger.info("Table            : " + config.table);
            logger.info("Region           : " + config.region);
            logger.info("Workgroup        : " + config.workgroup);
            logger.info("ES host          : " + config.esHost);
            logger.info("ES index prefix  : " + config.esIndexPrefix);
            logger.info("Concurrency      : " + config.concurrency);
            if (config.limit != null) logger.info("Limit            : " + config.limit + " ids");

            Set<String> alreadyHandled = LookupOutputWriter.loadAlreadyHandled(config.outputDir);
            if (!alreadyHandled.isEmpty()) {
                logger.info("Resuming: " + alreadyHandled.size()
                    + " source ids already handled in a prior run will be skipped.");
            }

            List<String> sourceIds = SourceIdCsvReader.read(config.inputFile, config.limit, alreadyHandled);
            logger.info("Loaded " + sourceIds.size() + " source ids to look up (after resume filtering).");
            if (sourceIds.isEmpty()) return;

            HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
            EsKeyResolver esResolver = new EsKeyResolver(httpClient, config.esHost, config.esIndexPrefix, logger);

            AthenaClient athenaClient = AthenaClient.builder().region(Region.of(config.region)).build();
            AthenaQueryRunner athenaRunner = new AthenaQueryRunner(athenaClient, config.database, config.workgroup, config.s3OutputLocation);
            AthenaCountLookup athenaLookup = new AthenaCountLookup(athenaRunner, config.table, config.reportingEntity);

            try (LookupOutputWriter output = new LookupOutputWriter(config.outputDir)) {
                Semaphore concurrencyLimiter = new Semaphore(config.concurrency);
                AtomicLong completed = new AtomicLong();
                AtomicLong resolvedCount = new AtomicLong();
                AtomicLong unresolvedCount = new AtomicLong();
                AtomicLong failedCount = new AtomicLong();
                long progressEvery = Math.max(1, sourceIds.size() / 50);
                long pipelineStart = System.currentTimeMillis();

                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    for (String sourceId : sourceIds) {
                        concurrencyLimiter.acquire();
                        executor.submit(() -> {
                            try {
                                processOne(sourceId, esResolver, athenaLookup, output, logger,
                                    resolvedCount, unresolvedCount, failedCount);
                            } finally {
                                concurrencyLimiter.release();
                                long done = completed.incrementAndGet();
                                if (done % progressEvery == 0 || done == sourceIds.size()) {
                                    double elapsedSec = (System.currentTimeMillis() - pipelineStart) / 1000.0;
                                    logger.info(String.format(
                                        "Progress: %d/%d ids (%.0f%%) | resolved=%d unresolved=%d failed=%d | elapsed=%.1fs",
                                        done, sourceIds.size(), 100.0 * done / sourceIds.size(),
                                        resolvedCount.get(), unresolvedCount.get(), failedCount.get(), elapsedSec));
                                }
                            }
                        });
                    }
                    // try-with-resources blocks here until every submitted task finishes.
                }

                logger.info(String.format("Done: %d resolved, %d unresolved, %d failed (out of %d).",
                    resolvedCount.get(), unresolvedCount.get(), failedCount.get(), sourceIds.size()));
            }
        }
    }

    private static void processOne(String sourceId, EsKeyResolver esResolver, AthenaCountLookup athenaLookup,
                                      LookupOutputWriter output, Logger logger,
                                      AtomicLong resolvedCount, AtomicLong unresolvedCount, AtomicLong failedCount) {
        List<String> keys;
        try {
            keys = esResolver.findKeys(sourceId);
        } catch (Exception e) {
            logger.warn("ES lookup failed for sourceId [" + sourceId + "]: " + e.getMessage());
            output.writeFailed(sourceId, "ES_LOOKUP", e.getMessage());
            failedCount.incrementAndGet();
            return;
        }

        if (keys.isEmpty()) {
            output.writeUnresolved(sourceId);
            unresolvedCount.incrementAndGet();
            return;
        }

        for (String key : keys) {
            String quadrimester = Quadrimester.fromKey(key);
            if (quadrimester == null) {
                logger.warn("Could not derive quadrimester for sourceId [" + sourceId + "], key [" + key + "]");
                output.writeFailed(sourceId, "QUADRIMESTER_DERIVE", "key didn't parse as YYYY/MM/...: " + key);
                failedCount.incrementAndGet();
                continue;
            }

            try {
                long athenaCount = athenaLookup.count(sourceId, quadrimester);
                output.writeResolved(sourceId, key, quadrimester, athenaCount);
                resolvedCount.incrementAndGet();
            } catch (Exception e) {
                logger.warn("Athena count failed for sourceId [" + sourceId + "], quadrimester [" + quadrimester + "]: " + e.getMessage());
                output.writeFailed(sourceId, "ATHENA_COUNT", e.getMessage());
                failedCount.incrementAndGet();
            }
        }
    }
}
