package com.smarsh.migration;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;

/**
 * Durable, dedicated record of every verification mismatch - distinct
 * from the scrolling run.log, so an operator can check what needs
 * attention after a run without re-reading the whole log.
 */
public class MismatchReporter implements AutoCloseable {

    static final String MISMATCH_FILE = "mismatched-quadrimesters.csv";

    private final PrintWriter writer;

    MismatchReporter(Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        boolean isNew = Files.notExists(outputDir.resolve(MISMATCH_FILE));
        writer = new PrintWriter(Files.newBufferedWriter(outputDir.resolve(MISMATCH_FILE), StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND));
        if (isNew) {
            writer.println("quadrimester,reporting_entity,temp_distinct_sources,temp_total_rows,stage2_distinct_sources,delta,timestamp");
            writer.flush();
        }
    }

    synchronized void report(String quadLabel, String entity, long tempDistinct, long tempTotal, long stageDistinct) {
        long delta = tempDistinct - stageDistinct;
        writer.printf("%s,%s,%d,%d,%d,%d,%s%n", quadLabel, entity, tempDistinct, tempTotal, stageDistinct, delta,
            LocalDateTime.now());
        writer.flush();
    }

    @Override
    public void close() {
        writer.close();
    }
}
