package com.smarsh.sourcelookup;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Streams source ids from a quoted or plain CSV, skipping a header row and any already-resolved ids. */
public class SourceIdCsvReader {

    static List<String> read(Path path, Integer limit, Set<String> alreadyHandled) throws IOException {
        List<String> ids = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(path)) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String id = firstColumn(line);
                if (first) {
                    first = false;
                    if (id.equalsIgnoreCase("source_id") || id.equalsIgnoreCase("sourceid")) continue;
                }
                if (id.isEmpty() || alreadyHandled.contains(id)) continue;

                ids.add(id);
                if (limit != null && ids.size() >= limit) break;
            }
        }
        return ids;
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
