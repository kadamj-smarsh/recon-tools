package com.smarsh.backlogger;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Thread-safe, append-mode, flush-per-write output files. These double as
 * both the audit trail and the resumability ledger (see loadAlreadyHandled).
 */
public class OutputWriters implements AutoCloseable {

    static final String SKIPPED_FILE = "skipped-already-in-es.csv";
    static final String SUBMITTED_FILE = "submitted-to-backlogger.csv";
    static final String FAILED_FILE = "failed-batches.csv";
    static final String SUMMARY_FILE = "run-summary.txt";

    private final PrintWriter skippedWriter;
    private final PrintWriter submittedWriter;
    private final PrintWriter failedWriter;
    private final Path outputDir;

    OutputWriters(Path outputDir) throws IOException {
        this.outputDir = outputDir;
        Files.createDirectories(outputDir);

        boolean skippedIsNew = Files.notExists(outputDir.resolve(SKIPPED_FILE));
        boolean submittedIsNew = Files.notExists(outputDir.resolve(SUBMITTED_FILE));
        boolean failedIsNew = Files.notExists(outputDir.resolve(FAILED_FILE));

        this.skippedWriter = openAppend(outputDir.resolve(SKIPPED_FILE));
        this.submittedWriter = openAppend(outputDir.resolve(SUBMITTED_FILE));
        this.failedWriter = openAppend(outputDir.resolve(FAILED_FILE));

        if (failedIsNew) {
            failedWriter.println("stage,batch_size,error_message,keys");
            failedWriter.flush();
        }
        // skipped/submitted files are plain one-key-per-line; no header needed,
        // but note "new" flags kept for clarity/future use.
        if (skippedIsNew || submittedIsNew) {
            // no-op: nothing to initialize beyond file creation
        }
    }

    private static PrintWriter openAppend(Path path) throws IOException {
        return new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND));
    }

    /**
     * Loads keys already recorded as "done" (either skipped-as-already-in-ES
     * or submitted-to-backlogger) from a prior run in outputDir, if present.
     * Failed keys are intentionally NOT included — a restart re-attempts them.
     */
    static Set<String> loadAlreadyHandled(Path outputDir) throws IOException {
        Set<String> handled = new HashSet<>();
        loadLinesInto(outputDir.resolve(SKIPPED_FILE), handled);
        loadLinesInto(outputDir.resolve(SUBMITTED_FILE), handled);
        return handled;
    }

    private static void loadLinesInto(Path path, Set<String> into) throws IOException {
        if (Files.notExists(path)) return;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) into.add(trimmed);
        }
    }

    synchronized void writeSkipped(List<String> keys) {
        for (String key : keys) skippedWriter.println(key);
        skippedWriter.flush();
    }

    synchronized void writeSubmitted(List<String> keys) {
        for (String key : keys) submittedWriter.println(key);
        submittedWriter.flush();
    }

    synchronized void writeFailed(String stage, List<String> keys, String errorMessage) {
        String safeError = errorMessage == null ? "" : errorMessage.replace("\"", "'").replace("\n", " ");
        String joinedKeys = String.join(";", keys);
        failedWriter.printf("%s,%d,\"%s\",\"%s\"%n", stage, keys.size(), safeError, joinedKeys);
        failedWriter.flush();
    }

    void writeSummary(RunStats stats, long elapsedMillis, Logger logger) throws IOException {
        String summary = stats.renderSummary(elapsedMillis);
        logger.info(summary);
        Files.writeString(outputDir.resolve(SUMMARY_FILE), summary, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        skippedWriter.close();
        submittedWriter.close();
        failedWriter.close();
    }
}
