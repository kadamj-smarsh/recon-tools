package com.smarsh.sourcelookup;

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
 * Writes:
 *   resolved-source-id-keys.csv   source_id,key,quadrimester,athena_count  (one row per ES key match)
 *   unresolved-source-ids.csv     source_id  (no ES match at all - Athena step skipped for these)
 *   failed-source-ids.csv         source_id,stage,error  (ES_LOOKUP or ATHENA_COUNT failure after retries)
 *
 * resolved + unresolved together form the resume ledger - a source_id in
 * either is "done" and skipped on the next run. Failed ids are never
 * added, so a rerun naturally retries them.
 */
public class LookupOutputWriter implements AutoCloseable {

    static final String RESOLVED_FILE = "resolved-source-id-keys.csv";
    static final String UNRESOLVED_FILE = "unresolved-source-ids.csv";
    static final String FAILED_FILE = "failed-source-ids.csv";

    private final PrintWriter resolved;
    private final PrintWriter unresolved;
    private final PrintWriter failed;

    LookupOutputWriter(Path outputDir) throws IOException {
        Files.createDirectories(outputDir);

        boolean resolvedIsNew = Files.notExists(outputDir.resolve(RESOLVED_FILE));
        boolean failedIsNew = Files.notExists(outputDir.resolve(FAILED_FILE));

        resolved = open(outputDir.resolve(RESOLVED_FILE));
        unresolved = open(outputDir.resolve(UNRESOLVED_FILE));
        failed = open(outputDir.resolve(FAILED_FILE));

        if (resolvedIsNew) {
            resolved.println("source_id,key,quadrimester,athena_count");
            resolved.flush();
        }
        if (failedIsNew) {
            failed.println("source_id,stage,error");
            failed.flush();
        }
    }

    private static PrintWriter open(Path path) throws IOException {
        return new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND));
    }

    static Set<String> loadAlreadyHandled(Path outputDir) throws IOException {
        Set<String> handled = new HashSet<>();
        loadFirstColumn(outputDir.resolve(RESOLVED_FILE), handled, true);
        loadFirstColumn(outputDir.resolve(UNRESOLVED_FILE), handled, false);
        return handled;
    }

    private static void loadFirstColumn(Path path, Set<String> into, boolean skipHeader) throws IOException {
        if (Files.notExists(path)) return;
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            if (skipHeader && trimmed.equals("source_id,key,quadrimester,athena_count")) continue;
            int comma = trimmed.indexOf(',');
            into.add(comma > 0 ? trimmed.substring(0, comma) : trimmed);
        }
    }

    synchronized void writeResolved(String sourceId, String key, String quadrimester, long athenaCount) {
        resolved.printf("%s,%s,%s,%d%n", sourceId, key, quadrimester, athenaCount);
        resolved.flush();
    }

    synchronized void writeUnresolved(String sourceId) {
        unresolved.println(sourceId);
        unresolved.flush();
    }

    synchronized void writeFailed(String sourceId, String stage, String error) {
        String safeError = error == null ? "" : error.replace("\"", "'").replace("\n", " ");
        failed.printf("%s,%s,\"%s\"%n", sourceId, stage, safeError);
        failed.flush();
    }

    @Override
    public void close() {
        resolved.close();
        unresolved.close();
        failed.close();
    }
}
