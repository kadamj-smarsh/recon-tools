package com.smarsh.backlogger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone one-off checker (separate from the backlogger reprocess
 * pipeline): for each sourceId in a CSV, runs a single ES _count query
 * matching "X-SMARSH-SOURCE-ID:{sourceId}" as a phrase inside
 * text.sys.content, and classifies the id by how many docs matched:
 *
 *   count == 0  -> zero-count-source-ids.csv  (not found at all - unexpected but tracked)
 *   count == 1  -> unique-source-ids.csv
 *   count  > 1  -> duplicate-source-ids.csv (sourceId,count)
 *   query error -> failed-source-ids.csv (sourceId,error) - single attempt, no retry
 *
 * Resumable: on startup, ids already present in unique/duplicate/zero-count
 * from a prior run (same --output-dir) are loaded and skipped - this is a
 * slow, per-id run (no batching possible - see below), so being able to
 * pick up where a prior run left off matters. Failed ids are NOT in this
 * ledger, so a rerun naturally retries them.
 *
 * Unlike the S3-key terms check, a sourceId can't be batched with others -
 * each query_string phrase match is inherently per-id - so this issues one
 * _count call per row, one per virtual thread, with overall concurrency to
 * ES bounded by --concurrency (start low: this is a heavier full-text
 * query with no real per-item date narrowing, unlike the S3 key checker).
 *
 * Run: java -cp target/backlogger-reprocess-1.0.0.jar
 *        com.smarsh.backlogger.DuplicateSourceIdChecker [options]
 */
public class DuplicateSourceIdChecker {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        long start = System.currentTimeMillis();

        try (Logger logger = new Logger(config.outputDir)) {
            logger.info("Input file       : " + config.inputFile);
            logger.info("ES host          : " + config.esHost);
            logger.info("ES index prefix  : " + config.esIndexPrefix);
            logger.info("Date range       : " + config.dateFrom + " .. " + config.dateTo);
            logger.info("Concurrency      : " + config.concurrency);
            logger.info("Output dir       : " + config.outputDir);
            if (config.limit != null) {
                logger.info("Limit            : " + config.limit + " ids");
            }

            Set<String> alreadyProcessed = loadAlreadyProcessed(config.outputDir);
            if (!alreadyProcessed.isEmpty()) {
                logger.info("Resuming: " + alreadyProcessed.size()
                    + " ids already processed in a prior run will be skipped.");
            }

            List<String> sourceIds = readSourceIds(config.inputFile, config.limit, alreadyProcessed);
            logger.info("Loaded " + sourceIds.size() + " source ids to check (after resume filtering).");

            HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

            try (Writers writers = new Writers(config.outputDir)) {
                Semaphore concurrencyLimiter = new Semaphore(config.concurrency);
                AtomicLong completed = new AtomicLong();
                long progressEvery = Math.max(1, sourceIds.size() / 50);

                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    for (String sourceId : sourceIds) {
                        concurrencyLimiter.acquire();
                        executor.submit(() -> {
                            try {
                                checkOne(httpClient, config, sourceId, writers, logger);
                            } finally {
                                concurrencyLimiter.release();
                                long done = completed.incrementAndGet();
                                if (done % progressEvery == 0 || done == sourceIds.size()) {
                                    double elapsedSec = (System.currentTimeMillis() - start) / 1000.0;
                                    logger.info(String.format("Progress: %d/%d ids (%.0f%%) | elapsed=%.1fs",
                                        done, sourceIds.size(), 100.0 * done / sourceIds.size(), elapsedSec));
                                }
                            }
                        });
                    }
                    // try-with-resources blocks here until every submitted task finishes.
                }

                logger.info(String.format(
                    "Done: %d unique, %d duplicate, %d zero-count, %d failed (out of %d total).",
                    writers.uniqueCount.get(), writers.duplicateCount.get(),
                    writers.zeroCountCounter.get(), writers.failedCount.get(), sourceIds.size()));
            }
        }

        System.out.printf("Total run time: %.1fs%n", (System.currentTimeMillis() - start) / 1000.0);
    }

    private static void checkOne(HttpClient client, Config config, String sourceId,
                                   Writers writers, Logger logger) {
        try {
            long count = countMatches(client, config, sourceId);
            if (count == 0) {
                writers.writeZeroCount(sourceId);
            } else if (count == 1) {
                writers.writeUnique(sourceId);
            } else {
                writers.writeDuplicate(sourceId, count);
            }
        } catch (Exception e) {
            logger.warn("Failed for sourceId [" + sourceId + "]: " + e.getMessage());
            writers.writeFailed(sourceId, e.getMessage());
        }
    }

    private static long countMatches(HttpClient client, Config config, String sourceId) throws Exception {
        String url = config.esHost + "/" + config.esIndexPrefix + "*/_count";
        String body = buildQuery(sourceId, config.dateFrom, config.dateTo);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IllegalStateException("ES _count returned HTTP " + response.statusCode() + ": "
                + truncate(response.body()));
        }

        JsonNode root = MAPPER.readTree(response.body());
        if (root.has("error")) {
            throw new IllegalStateException("ES _count returned an error: " + truncate(root.get("error").toString()));
        }

        return root.path("count").asLong(0);
    }

    private static String buildQuery(String sourceId, String dateFrom, String dateTo) {
        String phrase = escape("X-SMARSH-SOURCE-ID:" + sourceId);
        return "{"
            + "\"query\":{\"bool\":{\"must\":["
            +   "{\"query_string\":{\"query\":\"\\\"" + phrase + "\\\"\",\"fields\":[\"text.sys.content^1.0\"]}},"
            +   "{\"range\":{\"archivedTime\":{\"gte\":\"" + dateFrom + "\",\"lte\":\"" + dateTo + "\"}}}"
            + "]}}"
            + "}";
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.substring(0, Math.min(200, s.length()));
    }

    // ── CSV reading (same quoted-CSV pattern as CsvKeyReader) ─────────────────
    private static List<String> readSourceIds(Path path, Integer limit, Set<String> alreadyProcessed) throws Exception {
        List<String> ids = new java.util.ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(path)) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String id = firstColumn(line);
                if (first) {
                    first = false;
                    if (id.equalsIgnoreCase("source_id") || id.equalsIgnoreCase("sourceid")) continue;
                }
                if (id.isEmpty() || alreadyProcessed.contains(id)) continue;
                ids.add(id);
                if (limit != null && ids.size() >= limit) break;
            }
        }
        return ids;
    }

    /**
     * Loads ids already recorded as "done" (unique/duplicate/zero-count)
     * from a prior run in outputDir, if present. Failed ids are
     * intentionally NOT included - a restart re-attempts them.
     */
    private static Set<String> loadAlreadyProcessed(Path outputDir) throws Exception {
        Set<String> done = new HashSet<>();
        loadFirstColumnInto(outputDir.resolve("unique-source-ids.csv"), done);
        loadFirstColumnInto(outputDir.resolve("duplicate-source-ids.csv"), done);
        loadFirstColumnInto(outputDir.resolve("zero-count-source-ids.csv"), done);
        return done;
    }

    private static void loadFirstColumnInto(Path path, Set<String> into) throws Exception {
        if (Files.notExists(path)) return;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            int comma = trimmed.indexOf(',');
            into.add(comma > 0 ? trimmed.substring(0, comma) : trimmed);
        }
    }

    private static String firstColumn(String line) {
        if (line.startsWith("\"")) {
            int end = line.indexOf('"', 1);
            return end > 0 ? line.substring(1, end) : line;
        }
        int comma = line.indexOf(',');
        return comma > 0 ? line.substring(0, comma) : line;
    }

    // ── CLI config ──────────────────────────────────────────────────────────
    private static class Config {
        Path inputFile;
        String esHost;
        String esIndexPrefix;
        String dateFrom = "2025-01-01T00:00:00.000+0000";
        String dateTo = "2026-12-31T23:59:59.999+0000";
        int concurrency = 3;
        Integer limit;
        Path outputDir;

        static Config parse(String[] args) {
            Config c = new Config();
            String input = "../duplicates_source_ids.csv";
            String outputDir = "output-duplicates";
            String esHostOverride = null;
            String esIndexPrefixOverride = null;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                String value = (i + 1 < args.length) ? args[i + 1] : null;
                switch (arg) {
                    case "--input" -> { input = require(arg, value); i++; }
                    case "--es-host" -> { esHostOverride = require(arg, value); i++; }
                    case "--es-index-prefix" -> { esIndexPrefixOverride = require(arg, value); i++; }
                    case "--date-from" -> { c.dateFrom = require(arg, value); i++; }
                    case "--date-to" -> { c.dateTo = require(arg, value); i++; }
                    case "--concurrency" -> { c.concurrency = Integer.parseInt(require(arg, value)); i++; }
                    case "--limit" -> { c.limit = Integer.parseInt(require(arg, value)); i++; }
                    case "--output-dir" -> { outputDir = require(arg, value); i++; }
                    default -> throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }

            c.inputFile = Paths.get(input);
            c.outputDir = Paths.get(outputDir);
            c.esHost = resolve("es-host", "--es-host", esHostOverride, "BACKLOGGER_ES_HOST");
            c.esIndexPrefix = resolve("es-index-prefix", "--es-index-prefix", esIndexPrefixOverride, "BACKLOGGER_ES_INDEX_PREFIX");
            return c;
        }

        private static String resolve(String name, String flagName, String flagValue, String envVarName) {
            if (flagValue != null && !flagValue.isBlank()) return flagValue;
            String envValue = System.getenv(envVarName);
            if (envValue != null && !envValue.isBlank()) return envValue;
            throw new IllegalStateException(
                "No " + name + " configured. Set the " + flagName + " argument or the "
                    + envVarName + " environment variable.");
        }

        private static String require(String arg, String value) {
            if (value == null) {
                throw new IllegalArgumentException("Missing value for argument: " + arg);
            }
            return value;
        }
    }

    // ── Output writers ─────────────────────────────────────────────────────
    private static class Writers implements AutoCloseable {
        final PrintWriter unique;
        final PrintWriter duplicate;
        final PrintWriter zeroCount;
        final PrintWriter failed;

        final AtomicLong uniqueCount = new AtomicLong();
        final AtomicLong duplicateCount = new AtomicLong();
        final AtomicLong zeroCountCounter = new AtomicLong();
        final AtomicLong failedCount = new AtomicLong();

        Writers(Path outputDir) throws Exception {
            Files.createDirectories(outputDir);
            unique = open(outputDir.resolve("unique-source-ids.csv"));
            duplicate = open(outputDir.resolve("duplicate-source-ids.csv"));
            zeroCount = open(outputDir.resolve("zero-count-source-ids.csv"));
            failed = open(outputDir.resolve("failed-source-ids.csv"));
        }

        private static PrintWriter open(Path path) throws Exception {
            return new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND));
        }

        synchronized void writeUnique(String sourceId) {
            unique.println(sourceId);
            unique.flush();
            uniqueCount.incrementAndGet();
        }

        synchronized void writeDuplicate(String sourceId, long count) {
            duplicate.println(sourceId + "," + count);
            duplicate.flush();
            duplicateCount.incrementAndGet();
        }

        synchronized void writeZeroCount(String sourceId) {
            zeroCount.println(sourceId);
            zeroCount.flush();
            zeroCountCounter.incrementAndGet();
        }

        synchronized void writeFailed(String sourceId, String error) {
            String safeError = error == null ? "" : error.replace("\"", "'").replace("\n", " ");
            failed.printf("%s,\"%s\"%n", sourceId, safeError);
            failed.flush();
            failedCount.incrementAndGet();
        }

        @Override
        public void close() {
            unique.close();
            duplicate.close();
            zeroCount.close();
            failed.close();
        }
    }
}
