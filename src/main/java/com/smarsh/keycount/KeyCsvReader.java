package com.smarsh.keycount;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streams keys from the input CSV (first/only column, quoted or plain,
 * header skipped if it looks like one) without loading the whole file
 * into memory. Batches keys as it reads; keys already handled in a prior
 * run (per the resume ledger) are filtered out before they ever reach a
 * batch. Same pattern as com.smarsh.backlogger.CsvKeyReader.
 */
public class KeyCsvReader {

    static void streamBatches(Path path, int batchSize, Integer limit, Set<String> alreadyHandled,
                                AtomicLong keysRead, AtomicLong keysSkippedFromResume,
                                Consumer<List<String>> batchConsumer) throws IOException {
        List<String> current = new ArrayList<>(batchSize);
        long emitted = 0;

        try (BufferedReader br = Files.newBufferedReader(path)) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String key = firstColumn(line);
                if (first) {
                    first = false;
                    if (isHeader(key)) continue;
                }
                if (key.isEmpty()) continue;

                keysRead.incrementAndGet();

                if (alreadyHandled.contains(key)) {
                    keysSkippedFromResume.incrementAndGet();
                    continue;
                }

                current.add(key);
                emitted++;

                if (current.size() >= batchSize) {
                    batchConsumer.accept(current);
                    current = new ArrayList<>(batchSize);
                }

                if (limit != null && emitted >= limit) break;
            }
        }

        if (!current.isEmpty()) {
            batchConsumer.accept(current);
        }
    }

    private static boolean isHeader(String value) {
        return value.equalsIgnoreCase("key") || value.equalsIgnoreCase("keys")
            || value.equalsIgnoreCase("_col0") || value.equalsIgnoreCase("s3_key")
            || value.equalsIgnoreCase("s3key") || value.equalsIgnoreCase("s3 key");
    }

    private static String firstColumn(String line) {
        if (line.startsWith("\"")) {
            int end = line.indexOf('"', 1);
            return end > 0 ? line.substring(1, end) : line;
        }
        int comma = line.indexOf(',');
        return comma > 0 ? line.substring(0, comma) : line;
    }
}
