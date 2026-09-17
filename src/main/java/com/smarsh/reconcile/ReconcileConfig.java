package com.smarsh.reconcile;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses CLI args. Reuses the same BACKLOGGER_ES_HOST / BACKLOGGER_ES_INDEX_PREFIX
 * env vars as the backlogger/duplicate-checker tools (same ES cluster), and
 * the AWS SDK's default credential chain for Athena (AWS_ACCESS_KEY_ID /
 * AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN) - nothing AWS-specific handled here.
 *
 * Usage:
 *   java -cp backlogger-reprocess-1.0.0.jar com.smarsh.reconcile.Main
 *     --years <csv>              required, e.g. "2013" or "2013,2023"
 *     --reporting-entity <str>   required
 *     --database <name>          required (Athena database)
 *     --table <name>             default: tier2_migration_duplicate_stage2
 *     --region <str>             default: us-east-1
 *     --workgroup <str>          default: primary
 *     --es-host <url>            default: env BACKLOGGER_ES_HOST
 *     --es-index-prefix <str>    default: env BACKLOGGER_ES_INDEX_PREFIX
 *     --s3-output-location <uri> optional
 *     --output-csv <path>        default: ./reconcile-outstanding-keys.csv
 */
public class ReconcileConfig {

    final List<Integer> years;
    final String reportingEntity;
    final String database;
    final String table;
    final String region;
    final String workgroup;
    final String esHost;
    final String esIndexPrefix;
    final String s3OutputLocation;
    final Path outputCsv;

    private ReconcileConfig(List<Integer> years, String reportingEntity, String database, String table,
                              String region, String workgroup, String esHost, String esIndexPrefix,
                              String s3OutputLocation, Path outputCsv) {
        this.years = years;
        this.reportingEntity = reportingEntity;
        this.database = database;
        this.table = table;
        this.region = region;
        this.workgroup = workgroup;
        this.esHost = esHost;
        this.esIndexPrefix = esIndexPrefix;
        this.s3OutputLocation = s3OutputLocation;
        this.outputCsv = outputCsv;
    }

    static ReconcileConfig parse(String[] args) {
        String years = null;
        String reportingEntity = null;
        String database = null;
        String table = "tier2_migration_duplicate_stage2";
        String region = "us-east-1";
        String workgroup = "primary";
        String esHostOverride = null;
        String esIndexPrefixOverride = null;
        String s3OutputLocation = null;
        String outputCsv = "reconcile-outstanding-keys.csv";

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String value = (i + 1 < args.length) ? args[i + 1] : null;
            switch (arg) {
                case "--years" -> { years = require(arg, value); i++; }
                case "--reporting-entity" -> { reportingEntity = require(arg, value); i++; }
                case "--database" -> { database = require(arg, value); i++; }
                case "--table" -> { table = require(arg, value); i++; }
                case "--region" -> { region = require(arg, value); i++; }
                case "--workgroup" -> { workgroup = require(arg, value); i++; }
                case "--es-host" -> { esHostOverride = require(arg, value); i++; }
                case "--es-index-prefix" -> { esIndexPrefixOverride = require(arg, value); i++; }
                case "--s3-output-location" -> { s3OutputLocation = require(arg, value); i++; }
                case "--output-csv" -> { outputCsv = require(arg, value); i++; }
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        if (years == null || years.isBlank()) {
            throw new IllegalStateException("Missing required argument: --years <csv> (e.g. \"2013\" or \"2013,2023\")");
        }
        if (reportingEntity == null || reportingEntity.isBlank()) {
            throw new IllegalStateException("Missing required argument: --reporting-entity <value>");
        }
        if (database == null || database.isBlank()) {
            throw new IllegalStateException("Missing required argument: --database <name>");
        }

        List<Integer> parsedYears = new ArrayList<>();
        for (String token : years.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) parsedYears.add(Integer.parseInt(trimmed));
        }

        String esHost = resolve("es-host", "--es-host", esHostOverride, "BACKLOGGER_ES_HOST");
        String esIndexPrefix = resolve("es-index-prefix", "--es-index-prefix", esIndexPrefixOverride, "BACKLOGGER_ES_INDEX_PREFIX");

        return new ReconcileConfig(parsedYears, reportingEntity, database, table, region, workgroup,
            esHost, esIndexPrefix, s3OutputLocation, Paths.get(outputCsv));
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
