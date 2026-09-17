package com.smarsh.athena;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Parses CLI args. AWS credentials are NOT handled here - the AWS SDK's
 * default credential provider chain picks up the standard env vars
 * (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN)
 * automatically, so this tool never touches them directly.
 *
 * Usage:
 *   java -jar athena-year-key-count-1.0.0.jar
 *     --database <name>          required, no default
 *     --reporting-entity <str>   required, no default
 *     --table <name>             default: tier2_migration_duplicate_stage2
 *     --region <str>             default: us-east-1
 *     --workgroup <str>          default: primary
 *     --years <csv>              optional, e.g. "2023" or "2020,2021,pre-1970" - default: full 2000-2026 + special buckets
 *     --output-csv <path>        default: ./year-key-counts.csv
 *     --concurrency <int>        default: 3
 *     --s3-output-location <uri> optional, e.g. s3://bucket/prefix/ - only needed if your
 *                                workgroup has no default query-result location configured
 */
public class CliConfig {

    final String database;
    final String reportingEntity;
    final String table;
    final String region;
    final String workgroup;
    final List<YearBuckets.Bucket> buckets;
    final Path outputCsv;
    final int concurrency;
    final String s3OutputLocation; // nullable

    private CliConfig(String database, String reportingEntity, String table, String region, String workgroup,
                       List<YearBuckets.Bucket> buckets, Path outputCsv, int concurrency, String s3OutputLocation) {
        this.database = database;
        this.reportingEntity = reportingEntity;
        this.table = table;
        this.region = region;
        this.workgroup = workgroup;
        this.buckets = buckets;
        this.outputCsv = outputCsv;
        this.concurrency = concurrency;
        this.s3OutputLocation = s3OutputLocation;
    }

    static CliConfig parse(String[] args) {
        String database = null;
        String reportingEntity = null;
        String table = "tier2_migration_duplicate_stage2";
        String region = "us-east-1";
        String workgroup = "primary";
        String years = null;
        String outputCsv = "year-key-counts.csv";
        int concurrency = 3;
        String s3OutputLocation = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String value = (i + 1 < args.length) ? args[i + 1] : null;
            switch (arg) {
                case "--database" -> { database = require(arg, value); i++; }
                case "--reporting-entity" -> { reportingEntity = require(arg, value); i++; }
                case "--table" -> { table = require(arg, value); i++; }
                case "--region" -> { region = require(arg, value); i++; }
                case "--workgroup" -> { workgroup = require(arg, value); i++; }
                case "--years" -> { years = require(arg, value); i++; }
                case "--output-csv" -> { outputCsv = require(arg, value); i++; }
                case "--concurrency" -> { concurrency = Integer.parseInt(require(arg, value)); i++; }
                case "--s3-output-location" -> { s3OutputLocation = require(arg, value); i++; }
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        if (database == null || database.isBlank()) {
            throw new IllegalStateException("Missing required argument: --database <name>");
        }
        if (reportingEntity == null || reportingEntity.isBlank()) {
            throw new IllegalStateException("Missing required argument: --reporting-entity <value>");
        }

        List<YearBuckets.Bucket> buckets = (years != null) ? YearBuckets.parse(years) : YearBuckets.fullRange();

        return new CliConfig(database, reportingEntity, table, region, workgroup,
            buckets, Paths.get(outputCsv), concurrency, s3OutputLocation);
    }

    private static String require(String arg, String value) {
        if (value == null) {
            throw new IllegalArgumentException("Missing value for argument: " + arg);
        }
        return value;
    }
}
