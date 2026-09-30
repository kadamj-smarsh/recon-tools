package com.smarsh.migration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

/**
 * Resume ledger, same pattern as com.smarsh.athena.Main's per-bucket
 * manifest: a plain-text file, one completed quadrimester label
 * ("2025-T3") per line, loaded at startup and appended to only after a
 * quadrimester's final UNLOAD succeeds.
 */
public class MigrationLedger {

    static final String LEDGER_FILE = "processed-quadrimesters.txt";
    private static final Object LOCK = new Object();

    static Set<String> loadCompleted(Path outputDir) throws IOException {
        Set<String> labels = new HashSet<>();
        Path path = outputDir.resolve(LEDGER_FILE);
        if (Files.notExists(path)) return labels;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) labels.add(trimmed);
        }
        return labels;
    }

    static void markCompleted(Path outputDir, String label) throws IOException {
        synchronized (LOCK) {
            Files.writeString(outputDir.resolve(LEDGER_FILE), label + System.lineSeparator(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }
}
