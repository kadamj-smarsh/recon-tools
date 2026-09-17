package com.smarsh.reconcile;

import com.smarsh.athena.AthenaQueryRunner;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.io.PrintWriter;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.Executors;

/**
 * Reconciles ES vs Athena unique-key counts for reporting_entity, drilling
 * from month down to second only where counts actually differ, and writing
 * outstanding (ES-only / Athena-only) keys to a CSV once a mismatched
 * bucket's ES count drops under 10000 (see Reconciler for the full
 * algorithm). Sequential - see Reconciler's class javadoc for why.
 *
 * Run: java -cp target/backlogger-reprocess-1.0.0.jar com.smarsh.reconcile.Main
 *        --years 2013 --reporting-entity njfa.citi --database <db> [options]
 */
public class Main {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            run(ReconcileConfig.parse(args));
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

    private static void run(ReconcileConfig config) throws Exception {
        System.out.println("Years            : " + config.years);
        System.out.println("Reporting entity : " + config.reportingEntity);
        System.out.println("Database         : " + config.database);
        System.out.println("Table            : " + config.table);
        System.out.println("Region           : " + config.region);
        System.out.println("Workgroup        : " + config.workgroup);
        System.out.println("ES host          : " + config.esHost);
        System.out.println("ES index prefix  : " + config.esIndexPrefix);
        System.out.println("Output CSV       : " + config.outputCsv);

        HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();

        EsHistogramClient esClient = new EsHistogramClient(httpClient, config.esHost, config.esIndexPrefix, config.reportingEntity);

        AthenaClient athenaClient = AthenaClient.builder().region(Region.of(config.region)).build();
        AthenaQueryRunner athenaRunner = new AthenaQueryRunner(athenaClient, config.database, config.workgroup, config.s3OutputLocation);
        AthenaLevelQueries athenaQueries = new AthenaLevelQueries(athenaRunner, config.table, config.reportingEntity);

        boolean writeHeader = Files.notExists(config.outputCsv);
        try (PrintWriter csv = new PrintWriter(Files.newBufferedWriter(config.outputCsv, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
            if (writeHeader) {
                csv.println("level,window_start,window_end_exclusive,source,key");
                csv.flush();
            }

            ReconcileStats stats = new ReconcileStats();
            Reconciler reconciler = new Reconciler(esClient, athenaQueries, csv, stats);

            for (int year : config.years) {
                try {
                    reconciler.reconcileYear(year);
                } catch (Exception e) {
                    System.err.println("FAILED reconciling year " + year + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }

            System.out.println(stats.render());
        }
    }
}
