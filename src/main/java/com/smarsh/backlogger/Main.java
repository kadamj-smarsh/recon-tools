package com.smarsh.backlogger;

import java.net.http.HttpClient;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrator: streams keys from the input CSV in batches, and for each
 * batch runs "check ES -> filter -> submit missing to backlogger" as one
 * pipeline unit on a virtual thread. Resumable across restarts via the
 * ledger files in OutputWriters. Progress and errors are logged to both
 * the console and a persistent run.log file in --output-dir (Logger).
 */
public class Main {

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
        try (Logger logger = new Logger(config.outputDir)) {
            logger.info("Input file        : " + config.inputFile);
            logger.info("Batch size         : " + config.batchSize);
            logger.info("Concurrency        : " + config.concurrency);
            logger.info("Output dir         : " + config.outputDir);
            logger.info("ES host            : " + config.esHost);
            logger.info("Backlogger URL     : " + config.backloggerUrl);
            if (config.limit != null) {
                logger.info("Limit              : " + config.limit + " keys");
            }

            Set<String> alreadyHandled = OutputWriters.loadAlreadyHandled(config.outputDir);
            if (!alreadyHandled.isEmpty()) {
                logger.info("Resuming: " + alreadyHandled.size()
                    + " keys already handled in a prior run will be skipped.");
            }

            // Fast pre-scan (line count only, no parsing) so progress logging
            // has an approximate denominator. This is an ETA aid, not exact -
            // it doesn't account for keys the resume ledger will filter out.
            long totalLinesApprox = countLines(config.inputFile) - 1; // minus header
            long effectiveTotalApprox = config.limit != null
                ? Math.min(config.limit, Math.max(totalLinesApprox, 0))
                : Math.max(totalLinesApprox, 0);
            long totalBatchesApprox = Math.max(1, (effectiveTotalApprox + config.batchSize - 1) / config.batchSize);
            long progressEvery = Math.max(1, totalBatchesApprox / 50); // ~50 progress lines over the whole run
            logger.info("Approx. total batches: " + totalBatchesApprox
                + " (progress logged every " + progressEvery + " batches)");

            RunStats stats = new RunStats();
            AtomicLong batchesCompleted = new AtomicLong();
            long pipelineStart = System.currentTimeMillis();

            HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

            EsBatchChecker esChecker = new EsBatchChecker(httpClient, config.esHost, logger);
            BackloggerClient backloggerClient = new BackloggerClient(httpClient, config.backloggerUrl,
                config.backloggerToken, logger);

            try (OutputWriters outputWriters = new OutputWriters(config.outputDir)) {
                Semaphore concurrencyLimiter = new Semaphore(config.concurrency);

                try (ExecutorService pipelineExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
                    CsvKeyReader.streamBatches(config.inputFile, config.batchSize, config.limit,
                        alreadyHandled, stats, batch -> {
                            try {
                                concurrencyLimiter.acquire();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            pipelineExecutor.submit(() -> {
                                try {
                                    processBatch(batch, esChecker, backloggerClient, outputWriters, stats, logger);
                                } finally {
                                    concurrencyLimiter.release();
                                    long completed = batchesCompleted.incrementAndGet();
                                    if (completed % progressEvery == 0) {
                                        logProgress(logger, completed, totalBatchesApprox, stats, pipelineStart);
                                    }
                                }
                            });
                        });
                    // try-with-resources on pipelineExecutor blocks here (.close())
                    // until every submitted batch task has finished.
                }

                if (batchesCompleted.get() % progressEvery != 0) {
                    // Only log a final progress line if the last periodic one
                    // didn't already land exactly on the final count.
                    logProgress(logger, batchesCompleted.get(), totalBatchesApprox, stats, pipelineStart);
                }

                long elapsed = System.currentTimeMillis() - pipelineStart;
                outputWriters.writeSummary(stats, elapsed, logger);
            }
        }
    }

    private static void logProgress(Logger logger, long completed, long totalBatchesApprox,
                                      RunStats stats, long pipelineStart) {
        double elapsedSec = (System.currentTimeMillis() - pipelineStart) / 1000.0;
        double pct = totalBatchesApprox > 0 ? (100.0 * completed / totalBatchesApprox) : 0.0;
        logger.info(String.format(
            "Progress: %d/~%d batches (~%.0f%%) | skipped=%d submitted=%d failed=%d | elapsed=%.1fs",
            completed, totalBatchesApprox, pct,
            stats.keysAlreadyInEs.get(), stats.keysSubmitted.get(), stats.keysFailed.get(), elapsedSec));
    }

    private static long countLines(java.nio.file.Path path) throws java.io.IOException {
        try (var lines = Files.lines(path)) {
            return lines.count();
        }
    }

    private static void processBatch(List<String> batch, EsBatchChecker esChecker,
                                       BackloggerClient backloggerClient, OutputWriters outputWriters,
                                       RunStats stats, Logger logger) {
        List<String> missing;
        try {
            missing = esChecker.findMissingKeys(batch);
        } catch (BatchFailedException e) {
            // ES check itself failed: we don't know matched/missing, so the
            // whole batch is logged as failed. None of these keys were
            // written to the resume ledger, so a restart re-attempts them.
            logger.warn("Batch failed at ES_CHECK after all retries (" + batch.size()
                + " keys): " + e.getMessage());
            outputWriters.writeFailed(e.stage, batch, e.getMessage());
            stats.batchesFailed.incrementAndGet();
            stats.keysFailed.addAndGet(batch.size());
            return;
        }

        List<String> alreadyInEs = batch.stream()
            .filter(k -> !missing.contains(k))
            .toList();
        if (!alreadyInEs.isEmpty()) {
            outputWriters.writeSkipped(alreadyInEs);
            stats.keysAlreadyInEs.addAndGet(alreadyInEs.size());
        }

        if (!missing.isEmpty()) {
            try {
                backloggerClient.submit(missing);
                outputWriters.writeSubmitted(missing);
                stats.keysSubmitted.addAndGet(missing.size());
            } catch (BatchFailedException e) {
                // Only the keys that were actually missing (and therefore
                // attempted against backlogger) are logged as failed; the
                // alreadyInEs keys above are already safely in the ledger.
                logger.warn("Batch failed at BACKLOGGER_SUBMIT after all retries (" + missing.size()
                    + " keys): " + e.getMessage());
                outputWriters.writeFailed(e.stage, missing, e.getMessage());
                stats.batchesFailed.incrementAndGet();
                stats.keysFailed.addAndGet(missing.size());
                return;
            }
        }

        stats.batchesProcessed.incrementAndGet();
    }
}
