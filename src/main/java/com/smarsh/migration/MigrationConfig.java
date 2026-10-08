package com.smarsh.migration;

import com.smarsh.migration.QuadrimesterMonths.QuadKey;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parses CLI args. AWS credentials are NOT handled here - the AWS SDK's
 * default credential provider chain picks up the standard env vars
 * automatically.
 *
 * Usage:
 *   java -cp backlogger-reprocess-1.0.0.jar com.smarsh.migration.QuadrimesterMigrator
 *     --reporting-entity <str>   required
 *     --years <csv>              required, e.g. "2024,2025"
 *     --database <name>          required
 *     --quadrimesters <csv>      optional, e.g. "2025-T3" - narrows to specific quadrimesters,
 *                                must be a subset of what --years implies
 *     --stage2-table <name>      default: tier2_migration_duplicate_stage2
 *     --bucket <name>            required, no default (your migration S3 bucket)
 *     --region <str>             default: us-east-1
 *     --workgroup <str>          default: primary
 *     --s3-output-location <uri> optional
 *     --output-dir <path>        default: migration-output
 *     --concurrency <int>        default: 4 (bounds only the 4 monthly UNLOADs within one quadrimester)
 */
public class MigrationConfig {

    final String reportingEntity;
    final String database;
    final String stage2Table;
    final String bucket;
    final String region;
    final String workgroup;
    final String s3OutputLocation;
    final Path outputDir;
    final int concurrency;
    final List<QuadKey> quadrimesters;

    private MigrationConfig(String reportingEntity, String database, String stage2Table, String bucket,
                              String region, String workgroup, String s3OutputLocation, Path outputDir,
                              int concurrency, List<QuadKey> quadrimesters) {
        this.reportingEntity = reportingEntity;
        this.database = database;
        this.stage2Table = stage2Table;
        this.bucket = bucket;
        this.region = region;
        this.workgroup = workgroup;
        this.s3OutputLocation = s3OutputLocation;
        this.outputDir = outputDir;
        this.concurrency = concurrency;
        this.quadrimesters = quadrimesters;
    }

    static MigrationConfig parse(String[] args) {
        String reportingEntity = null;
        String years = null;
        String database = null;
        String quadrimesters = null;
        String stage2Table = "tier2_migration_duplicate_stage2";
        String bucket = null;
        String region = "us-east-1";
        String workgroup = "primary";
        String s3OutputLocation = null;
        String outputDir = "migration-output";
        int concurrency = 4;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String value = (i + 1 < args.length) ? args[i + 1] : null;
            switch (arg) {
                case "--reporting-entity" -> { reportingEntity = require(arg, value); i++; }
                case "--years" -> { years = require(arg, value); i++; }
                case "--database" -> { database = require(arg, value); i++; }
                case "--quadrimesters" -> { quadrimesters = require(arg, value); i++; }
                case "--stage2-table" -> { stage2Table = require(arg, value); i++; }
                case "--bucket" -> { bucket = require(arg, value); i++; }
                case "--region" -> { region = require(arg, value); i++; }
                case "--workgroup" -> { workgroup = require(arg, value); i++; }
                case "--s3-output-location" -> { s3OutputLocation = require(arg, value); i++; }
                case "--output-dir" -> { outputDir = require(arg, value); i++; }
                case "--concurrency" -> { concurrency = Integer.parseInt(require(arg, value)); i++; }
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        if (reportingEntity == null || reportingEntity.isBlank()) {
            throw new IllegalStateException("Missing required argument: --reporting-entity <value>");
        }
        if (years == null || years.isBlank()) {
            throw new IllegalStateException("Missing required argument: --years <csv> (e.g. \"2024,2025\")");
        }
        if (database == null || database.isBlank()) {
            throw new IllegalStateException("Missing required argument: --database <name>");
        }
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalStateException("Missing required argument: --bucket <name>");
        }

        List<Integer> parsedYears = new ArrayList<>();
        for (String token : years.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) parsedYears.add(Integer.parseInt(trimmed));
        }
        List<QuadKey> allQuads = QuadrimesterMonths.expand(parsedYears);

        List<QuadKey> resolvedQuads;
        if (quadrimesters != null && !quadrimesters.isBlank()) {
            List<QuadKey> requested = new ArrayList<>();
            for (String token : quadrimesters.split(",")) {
                String trimmed = token.trim();
                if (!trimmed.isEmpty()) requested.add(QuadrimesterMonths.parseLabel(trimmed));
            }
            Set<String> allowedLabels = allQuads.stream().map(QuadKey::label).collect(Collectors.toSet());
            for (QuadKey q : requested) {
                if (!allowedLabels.contains(q.label())) {
                    throw new IllegalStateException("--quadrimesters value \"" + q.label()
                        + "\" is not one of the quadrimesters implied by --years " + years);
                }
            }
            resolvedQuads = requested;
        } else {
            resolvedQuads = allQuads;
        }

        return new MigrationConfig(reportingEntity, database, stage2Table, bucket, region, workgroup,
            s3OutputLocation, Paths.get(outputDir), concurrency, resolvedQuads);
    }

    private static String require(String arg, String value) {
        if (value == null) {
            throw new IllegalArgumentException("Missing value for argument: " + arg);
        }
        return value;
    }
}
