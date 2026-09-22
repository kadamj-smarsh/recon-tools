package com.smarsh.keycount;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Writes key-counts.csv (key,count) and failed-batches.csv, both
 * append-mode with a flush per write. key-counts.csv doubles as the
 * resume ledger: any key already present in it (from a prior run, same
 * --output-dir) is skipped on the next run - failed keys are never
 * written there, so a rerun naturally retries them.
 */
public class OutputWriter implements AutoCloseable {

    static final String COUNTS_FILE = "key-counts.csv";
    static final String FAILED_FILE = "failed-batches.csv";

    private final PrintWriter countsWriter;
    private final PrintWriter failedWriter;

    OutputWriter(Path outputDir) throws IOException {
        Files.createDirectories(outputDir);

        boolean countsIsNew = Files.notExists(outputDir.resolve(COUNTS_FILE));
        boolean failedIsNew = Files.notExists(outputDir.resolve(FAILED_FILE));

        countsWriter = open(outputDir.resolve(COUNTS_FILE));
        failedWriter = open(outputDir.resolve(FAILED_FILE));

        if (countsIsNew) {
            countsWriter.println("key,count");
            countsWriter.flush();
        }
        if (failedIsNew) {
            failedWriter.println("batch_size,error_message,keys");
            failedWriter.flush();
        }
    }

    private static PrintWriter open(Path path) throws IOException {
        return new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND));
    }

    static Set<String> loadAlreadyHandled(Path outputDir) throws IOException {
        Set<String> handled = new HashSet<>();
        Path path = outputDir.resolve(COUNTS_FILE);
        if (Files.notExists(path)) return handled;
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.equals("key,count")) continue;
            int comma = trimmed.lastIndexOf(',');
            if (comma > 0) handled.add(trimmed.substring(0, comma));
        }
        return handled;
    }

    synchronized void writeCounts(Map<String, Long> keyCounts) {
        for (Map.Entry<String, Long> e : keyCounts.entrySet()) {
            countsWriter.println(e.getKey() + "," + e.getValue());
        }
        countsWriter.flush();
    }

    synchronized void writeFailed(List<String> keys, String errorMessage) {
        String safeError = errorMessage == null ? "" : errorMessage.replace("\"", "'").replace("\n", " ");
        failedWriter.printf("%d,\"%s\",\"%s\"%n", keys.size(), safeError, String.join(";", keys));
        failedWriter.flush();
    }

    @Override
    public void close() {
        countsWriter.close();
        failedWriter.close();
    }
}
