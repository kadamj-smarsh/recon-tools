package com.smarsh.sourcelookup;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Parses CLI args. Reuses the same BACKLOGGER_ES_HOST / BACKLOGGER_ES_INDEX_PREFIX
 * env vars as the other ES-touching tools, plus the AWS SDK's default
 * credential chain for Athena (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
 * AWS_SESSION_TOKEN) - nothing AWS-specific handled here.
 *
 * Usage:
 *   java -cp backlogger-reprocess-1.0.0.jar com.smarsh.sourcelookup.SourceIdAthenaLookup
 *     --input <path>              required
 *     --reporting-entity <str>    required
 *     --database <name>           required (Athena database)
 *     --table <name>              default: tier2_migration_duplicate_stage2
 *     --region <str>              default: us-east-1
 *     --workgroup <str>           default: primary
 *     --s3-output-location <uri> optional
 *     --es-host <url>             default: env BACKLOGGER_ES_HOST
 *     --es-index-prefix <str>     default: env BACKLOGGER_ES_INDEX_PREFIX
 *     --output-dir <path>         default: output-sourcelookup
 *     --concurrency <int>         default: 5
 *     --limit <int>               optional
 */
public class LookupConfig {

    final Path inputFile;
    final String reportingEntity;
    final String database;
    final String table;
    final String region;
    final String workgroup;
    final String s3OutputLocation;
    final String esHost;
    final String esIndexPrefix;
    final Path outputDir;
    final int concurrency;
    final Integer limit;

    private LookupConfig(Path inputFile, String reportingEntity, String database, String table, String region,
                           String workgroup, String s3OutputLocation, String esHost, String esIndexPrefix,
                           Path outputDir, int concurrency, Integer limit) {
        this.inputFile = inputFile;
        this.reportingEntity = reportingEntity;
        this.database = database;
        this.table = table;
        this.region = region;
        this.workgroup = workgroup;
        this.s3OutputLocation = s3OutputLocation;
        this.esHost = esHost;
        this.esIndexPrefix = esIndexPrefix;
        this.outputDir = outputDir;
        this.concurrency = concurrency;
        this.limit = limit;
    }

    static LookupConfig parse(String[] args) {
        String input = null;
        String reportingEntity = null;
        String database = null;
        String table = "tier2_migration_duplicate_stage2";
        String region = "us-east-1";
        String workgroup = "primary";
        String s3OutputLocation = null;
        String esHostOverride = null;
        String esIndexPrefixOverride = null;
        String outputDir = "output-sourcelookup";
        int concurrency = 5;
        Integer limit = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String value = (i + 1 < args.length) ? args[i + 1] : null;
            switch (arg) {
                case "--input" -> { input = require(arg, value); i++; }
                case "--reporting-entity" -> { reportingEntity = require(arg, value); i++; }
                case "--database" -> { database = require(arg, value); i++; }
                case "--table" -> { table = require(arg, value); i++; }
                case "--region" -> { region = require(arg, value); i++; }
                case "--workgroup" -> { workgroup = require(arg, value); i++; }
                case "--s3-output-location" -> { s3OutputLocation = require(arg, value); i++; }
                case "--es-host" -> { esHostOverride = require(arg, value); i++; }
                case "--es-index-prefix" -> { esIndexPrefixOverride = require(arg, value); i++; }
                case "--output-dir" -> { outputDir = require(arg, value); i++; }
                case "--concurrency" -> { concurrency = Integer.parseInt(require(arg, value)); i++; }
                case "--limit" -> { limit = Integer.parseInt(require(arg, value)); i++; }
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        if (input == null || input.isBlank()) {
            throw new IllegalStateException("Missing required argument: --input <path>");
        }
        if (reportingEntity == null || reportingEntity.isBlank()) {
            throw new IllegalStateException("Missing required argument: --reporting-entity <value>");
        }
        if (database == null || database.isBlank()) {
            throw new IllegalStateException("Missing required argument: --database <name>");
        }

        String esHost = resolve("es-host", "--es-host", esHostOverride, "BACKLOGGER_ES_HOST");
        String esIndexPrefix = resolve("es-index-prefix", "--es-index-prefix", esIndexPrefixOverride, "BACKLOGGER_ES_INDEX_PREFIX");

        return new LookupConfig(Paths.get(input), reportingEntity, database, table, region, workgroup,
            s3OutputLocation, esHost, esIndexPrefix, Paths.get(outputDir), concurrency, limit);
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
