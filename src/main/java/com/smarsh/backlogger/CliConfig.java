package com.smarsh.backlogger;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Parses CLI args and holds all run configuration.
 *
 * Usage:
 *   java -jar backlogger-reprocess-1.0.0.jar
 *     --input <path>            default: swfaciti_keys_to_reprocess.csv
 *     --batch-size <int>        default: 500
 *     --concurrency <int>       default: 100
 *     --output-dir <path>       default: ./output
 *     --limit <int>             optional
 *     --es-host <url>           default: http://10.30.146.93:9200
 *     --backlogger-url <url>    default: ea-tier2-backlogger-v2 replayKeys URL
 */
public class CliConfig {

    static final String DEFAULT_INPUT = "swfaciti_keys_to_reprocess.csv";
    static final int DEFAULT_BATCH_SIZE = 500;
    static final int DEFAULT_CONCURRENCY = 100;
    static final String DEFAULT_OUTPUT_DIR = "output";
    static final String DEFAULT_ES_HOST = "http://10.30.146.93:9200";
    static final String DEFAULT_BACKLOGGER_URL =
        "https://ea-tier2-backlogger-v2-rest-rmaas.ea.internal.citi.us-east-1.aws.smarsh.cloud/backlogger/replayKeys";

    final Path inputFile;
    final int batchSize;
    final int concurrency;
    final Path outputDir;
    final Integer limit; // null = no limit
    final String esHost;
    final String backloggerUrl;
    final String backloggerToken;

    private CliConfig(Path inputFile, int batchSize, int concurrency, Path outputDir,
                       Integer limit, String esHost, String backloggerUrl, String backloggerToken) {
        this.inputFile = inputFile;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.outputDir = outputDir;
        this.limit = limit;
        this.esHost = esHost;
        this.backloggerUrl = backloggerUrl;
        this.backloggerToken = backloggerToken;
    }

    static CliConfig parse(String[] args) {
        String input = DEFAULT_INPUT;
        int batchSize = DEFAULT_BATCH_SIZE;
        int concurrency = DEFAULT_CONCURRENCY;
        String outputDir = DEFAULT_OUTPUT_DIR;
        Integer limit = null;
        String esHost = DEFAULT_ES_HOST;
        String backloggerUrl = DEFAULT_BACKLOGGER_URL;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String value = (i + 1 < args.length) ? args[i + 1] : null;
            switch (arg) {
                case "--input" -> { input = require(arg, value); i++; }
                case "--batch-size" -> { batchSize = Integer.parseInt(require(arg, value)); i++; }
                case "--concurrency" -> { concurrency = Integer.parseInt(require(arg, value)); i++; }
                case "--output-dir" -> { outputDir = require(arg, value); i++; }
                case "--limit" -> { limit = Integer.parseInt(require(arg, value)); i++; }
                case "--es-host" -> { esHost = require(arg, value); i++; }
                case "--backlogger-url" -> { backloggerUrl = require(arg, value); i++; }
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        String token = System.getenv("BACKLOGGER_TOKEN");
        if (token == null || token.isBlank()) {
            throw new IllegalStateException(
                "Environment variable BACKLOGGER_TOKEN is not set. " +
                "Set it to a valid Bearer token before running this tool.");
        }

        return new CliConfig(Paths.get(input), batchSize, concurrency, Paths.get(outputDir),
            limit, esHost, backloggerUrl, token);
    }

    private static String require(String arg, String value) {
        if (value == null) {
            throw new IllegalArgumentException("Missing value for argument: " + arg);
        }
        return value;
    }
}
