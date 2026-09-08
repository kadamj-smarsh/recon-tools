package com.smarsh.backlogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Streams S3 keys from the input CSV (first/only column, quoted, header
 * "_col0" from an Athena CTAS export) without loading the whole file into
 * memory. Batches keys as it reads and hands each full batch to a consumer
 * as soon as it's ready.
 */
public class CsvKeyReader {

    static void streamBatches(Path path, int batchSize, Integer limit,
                               Set<String> alreadyHandled, RunStats stats,
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
                    if (isHeader(key)) continue; // skip header row (e.g. "_col0")
                }
                if (key.isEmpty()) continue;

                stats.keysRead.incrementAndGet();

                if (alreadyHandled.contains(key)) {
                    stats.keysSkippedFromResume.incrementAndGet();
                    continue; // already done in a prior run; never re-batch
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
        return value.equalsIgnoreCase("_col0") || value.equalsIgnoreCase("key")
            || value.equalsIgnoreCase("s3_key") || value.equalsIgnoreCase("s3key")
            || value.equalsIgnoreCase("s3 key");
    }

    // Same quoted-CSV unwrap logic as the sibling es-key-search tool.
    static String firstColumn(String line) {
        if (line.startsWith("\"")) {
            int end = line.indexOf('"', 1);
            return end > 0 ? line.substring(1, end) : line;
        }
        int comma = line.indexOf(',');
        return comma > 0 ? line.substring(0, comma) : line;
    }
}
