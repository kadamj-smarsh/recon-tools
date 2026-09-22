package com.smarsh.keycount;

import com.smarsh.backlogger.Logger;

import java.net.http.HttpClient;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * For each batch of keys from the input CSV, runs one ES terms+aggregation
 * _search to get every key's doc_count in that batch in a single request
 * (see EsKeyCountBatchChecker), and appends key,count rows to a CSV.
 * Resumable (same ledger pattern as the backlogger/duplicate-checker
 * tools): keys already in key-counts.csv from a prior run are skipped.
 *
 * Run: java -cp target/backlogger-reprocess-1.0.0.jar com.smarsh.keycount.Main
 *        --input keys_to_check.csv [options]
 */
public class Main {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            run(KeyCountConfig.parse(args));
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

    private static void run(KeyCountConfig config) throws Exception {
        try (Logger logger = new Logger(config.outputDir)) {
            logger.info("Input file       : " + config.inputFile);
            logger.info("Batch size       : " + config.batchSize);
            logger.info("Concurrency      : " + config.concurrency);
            logger.info("Output dir       : " + config.outputDir);
            logger.info("ES host          : " + config.esHost);
            logger.info("ES index prefix  : " + config.esIndexPrefix);
            if (config.limit != null) {
                logger.info("Limit            : " + config.limit + " keys");
            }

            Set<String> alreadyHandled = OutputWriter.loadAlreadyHandled(config.outputDir);
            if (!alreadyHandled.isEmpty()) {
                logger.info("Resuming: " + alreadyHandled.size()
                    + " keys already counted in a prior run will be skipped.");
            }

            AtomicLong keysRead = new AtomicLong();
            AtomicLong keysSkippedFromResume = new AtomicLong();
            AtomicLong keysCounted = new AtomicLong();
            AtomicLong duplicateKeys = new AtomicLong();
            AtomicLong batchesFailed = new AtomicLong();
            AtomicLong batchesProcessed = new AtomicLong();

            long pipelineStart = System.currentTimeMillis();

            HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

            EsKeyCountBatchChecker checker = new EsKeyCountBatchChecker(httpClient, config.esHost, config.esIndexPrefix, logger);

            try (OutputWriter output = new OutputWriter(config.outputDir)) {
                Semaphore concurrencyLimiter = new Semaphore(config.concurrency);

                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    KeyCsvReader.streamBatches(config.inputFile, config.batchSize, config.limit,
                        alreadyHandled, keysRead, keysSkippedFromResume, batch -> {
                            try {
                                concurrencyLimiter.acquire();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            executor.submit(() -> {
                                try {
                                    Map<String, Long> counts = checker.countBatch(batch);
                                    output.writeCounts(counts);
                                    keysCounted.addAndGet(counts.size());
                                    counts.values().forEach(c -> { if (c > 1) duplicateKeys.incrementAndGet(); });
                                    batchesProcessed.incrementAndGet();
                                } catch (Exception e) {
                                    logger.warn("Batch failed after all retries (" + batch.size() + " keys): " + e.getMessage());
                                    output.writeFailed(batch, e.getMessage());
                                    batchesFailed.incrementAndGet();
                                } finally {
                                    concurrencyLimiter.release();
                                    long done = batchesProcessed.get() + batchesFailed.get();
                                    logger.info(String.format("Progress: %d batches done (%d failed) | keysCounted=%d duplicates=%d",
                                        done, batchesFailed.get(), keysCounted.get(), duplicateKeys.get()));
                                }
                            });
                        });
                    // try-with-resources blocks here until every submitted batch finishes.
                }

                long elapsed = System.currentTimeMillis() - pipelineStart;
                String summary = """
                    ==== Run Summary ====
                    Keys read from input CSV      : %d
                    Keys skipped (already counted): %d
                    Batches processed             : %d
                    Batches failed                : %d
                    Keys counted this run         : %d
                    Keys with count > 1 (dupes)   : %d
                    Elapsed time                  : %.1fs
                    ======================
                    """.formatted(keysRead.get(), keysSkippedFromResume.get(), batchesProcessed.get(),
                        batchesFailed.get(), keysCounted.get(), duplicateKeys.get(), elapsed / 1000.0);
                logger.info(summary);
            }
        }
    }
}
