package com.smarsh.keycount;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Parses CLI args. Reuses the same BACKLOGGER_ES_HOST / BACKLOGGER_ES_INDEX_PREFIX
 * env vars as the backlogger/duplicate-checker tools (same ES cluster).
 *
 * Usage:
 *   java -cp backlogger-reprocess-1.0.0.jar com.smarsh.keycount.Main
 *     --input <path>             required, e.g. keys_to_check.csv
 *     --batch-size <int>         default: 500
 *     --concurrency <int>        default: 10
 *     --output-dir <path>        default: ./output-keycount
 *     --limit <int>              optional
 *     --es-host <url>            default: env BACKLOGGER_ES_HOST
 *     --es-index-prefix <str>    default: env BACKLOGGER_ES_INDEX_PREFIX
 */
public class KeyCountConfig {

    final Path inputFile;
    final int batchSize;
    final int concurrency;
    final Path outputDir;
    final Integer limit;
    final String esHost;
    final String esIndexPrefix;

    private KeyCountConfig(Path inputFile, int batchSize, int concurrency, Path outputDir, Integer limit,
                             String esHost, String esIndexPrefix) {
        this.inputFile = inputFile;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.outputDir = outputDir;
        this.limit = limit;
        this.esHost = esHost;
        this.esIndexPrefix = esIndexPrefix;
    }

    static KeyCountConfig parse(String[] args) {
        String input = null;
        int batchSize = 500;
        int concurrency = 10;
        String outputDir = "output-keycount";
        Integer limit = null;
        String esHostOverride = null;
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
                case "--es-index-prefix" -> { esIndexPrefixOverride = require(arg, value); i++; }
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        if (input == null || input.isBlank()) {
            throw new IllegalStateException("Missing required argument: --input <path>");
        }

        String esHost = resolve("es-host", "--es-host", esHostOverride, "BACKLOGGER_ES_HOST");
        String esIndexPrefix = resolve("es-index-prefix", "--es-index-prefix", esIndexPrefixOverride, "BACKLOGGER_ES_INDEX_PREFIX");

        return new KeyCountConfig(Paths.get(input), batchSize, concurrency, Paths.get(outputDir), limit,
            esHost, esIndexPrefix);
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
