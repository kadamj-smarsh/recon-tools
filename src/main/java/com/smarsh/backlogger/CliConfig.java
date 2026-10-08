package com.smarsh.backlogger;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Parses CLI args and holds all run configuration.
 *
 * Usage:
 *   java -jar backlogger-reprocess-1.0.0.jar
 *     --input <path>            default: keys_to_reprocess.csv
 *     --batch-size <int>        default: 500
 *     --concurrency <int>       default: 100
 *     --output-dir <path>       default: ./output
 *     --limit <int>             optional
 *     --es-host <url>           optional override of BACKLOGGER_ES_HOST
 *     --backlogger-url <url>    optional override of BACKLOGGER_REPLAY_URL
 *     --token-url <url>         optional override of BACKLOGGER_TOKEN_URL
 *     --es-index-prefix <str>   optional override of BACKLOGGER_ES_INDEX_PREFIX
 *
 * Required environment variables (no defaults):
 *   BACKLOGGER_ES_HOST          e.g. http://10.10.100.10:9200
 *   BACKLOGGER_REPLAY_URL       the backlogger replayKeys endpoint
 *   BACKLOGGER_TOKEN_URL        the UAA oauth/token endpoint
 *   BACKLOGGER_CLIENT_ID        OAuth client_credentials client_id
 *   BACKLOGGER_CLIENT_SECRET    OAuth client_credentials client_secret
 *   BACKLOGGER_ES_INDEX_PREFIX  e.g. "rmaas-tier2-" (EsBatchChecker appends
 *                               each batch's own YYYY-MM-* to this)
 */
public class CliConfig {

    static final String DEFAULT_INPUT = "keys_to_reprocess.csv";
    static final int DEFAULT_BATCH_SIZE = 500;
    static final int DEFAULT_CONCURRENCY = 100;
    static final String DEFAULT_OUTPUT_DIR = "output";

    final Path inputFile;
    final int batchSize;
    final int concurrency;
    final Path outputDir;
    final Integer limit; // null = no limit
    final String esHost;
    final String backloggerUrl;
    final String tokenUrl;
    final String esIndexPrefix;
    final String clientId;
    final String clientSecret;

    private CliConfig(Path inputFile, int batchSize, int concurrency, Path outputDir, Integer limit,
                       String esHost, String backloggerUrl, String tokenUrl, String esIndexPrefix,
                       String clientId, String clientSecret) {
        this.inputFile = inputFile;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.outputDir = outputDir;
        this.limit = limit;
        this.esHost = esHost;
        this.backloggerUrl = backloggerUrl;
        this.tokenUrl = tokenUrl;
        this.esIndexPrefix = esIndexPrefix;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    static CliConfig parse(String[] args) {
        String input = DEFAULT_INPUT;
        int batchSize = DEFAULT_BATCH_SIZE;
        int concurrency = DEFAULT_CONCURRENCY;
        String outputDir = DEFAULT_OUTPUT_DIR;
        Integer limit = null;
        String esHostOverride = null;
        String backloggerUrlOverride = null;
        String tokenUrlOverride = null;
        String esIndexPrefixOverride = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String value = (i + 1 < args.length) ? args[i + 1] : null;
            switch (arg) {
                case "--input" -> { input = require(arg, value); i++; }
                case "--batch-size" -> { batchSize = Integer.parseInt(require(arg, value)); i++; }
                case "--concurrency" -> { concurrency = Integer.parseInt(require(arg, value)); i++; }
                case "--output-dir" -> { outputDir = require(arg, value); i++; }
                case "--limit" -> { limit = Integer.parseInt(require(arg, value)); i++; }
                case "--es-host" -> { esHostOverride = require(arg, value); i++; }
                case "--backlogger-url" -> { backloggerUrlOverride = require(arg, value); i++; }
                case "--token-url" -> { tokenUrlOverride = require(arg, value); i++; }
                case "--es-index-prefix" -> { esIndexPrefixOverride = require(arg, value); i++; }
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        String esHost = resolve("es-host", "--es-host", esHostOverride, "BACKLOGGER_ES_HOST");
        String backloggerUrl = resolve("backlogger-url", "--backlogger-url", backloggerUrlOverride, "BACKLOGGER_REPLAY_URL");
        String tokenUrl = resolve("token-url", "--token-url", tokenUrlOverride, "BACKLOGGER_TOKEN_URL");
        String esIndexPrefix = resolve("es-index-prefix", "--es-index-prefix", esIndexPrefixOverride, "BACKLOGGER_ES_INDEX_PREFIX");
        String clientId = requireEnv("BACKLOGGER_CLIENT_ID");
        String clientSecret = requireEnv("BACKLOGGER_CLIENT_SECRET");

        return new CliConfig(Paths.get(input), batchSize, concurrency, Paths.get(outputDir), limit,
            esHost, backloggerUrl, tokenUrl, esIndexPrefix, clientId, clientSecret);
    }

    /** CLI flag wins if given; otherwise falls back to the env var; fails fast if neither is set. */
    private static String resolve(String name, String flagName, String flagValue, String envVarName) {
        if (flagValue != null && !flagValue.isBlank()) return flagValue;
        String envValue = System.getenv(envVarName);
        if (envValue != null && !envValue.isBlank()) return envValue;
        throw new IllegalStateException(
            "No " + name + " configured. Set the " + flagName + " argument or the "
                + envVarName + " environment variable.");
    }

    private static String requireEnv(String envVarName) {
        String value = System.getenv(envVarName);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                "Environment variable " + envVarName + " is not set. Set it before running this tool.");
        }
        return value;
    }

    private static String require(String arg, String value) {
        if (value == null) {
            throw new IllegalArgumentException("Missing value for argument: " + arg);
        }
        return value;
    }
}
