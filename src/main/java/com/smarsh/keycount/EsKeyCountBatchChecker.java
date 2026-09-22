package com.smarsh.keycount;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smarsh.backlogger.Logger;
import com.smarsh.backlogger.RetryExecutor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checks a batch of keys against Elasticsearch's exact "key" field via
 * one or more terms-filtered _search calls per batch, tallying doc counts
 * per key client-side from the returned hits.
 *
 * NOT an aggregation, despite "key" being an exact-match field: real
 * testing showed this cluster's "key" field has doc_values disabled
 * ("fielddata is unsupported on fields of type [keyword]"), which a
 * terms aggregation requires - so aggregating is off the table here.
 * Fetching hits and counting client-side sidesteps that entirely (hit
 * retrieval only needs the inverted index, not doc_values), and pages via
 * search_after sorted by "_doc" (Lucene's native order - also
 * doc_values-free) if a batch has enough matching/duplicate docs to fill
 * one page.
 *
 * Still far faster than DuplicateSourceIdChecker's approach for source
 * ids: "key" is an exact filter, one query per BATCH (500 keys) rather
 * than one query per individual id.
 */
public class EsKeyCountBatchChecker {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int PAGE_SIZE = 10000; // ES's max result window

    private final HttpClient client;
    private final String esHost;
    private final String esIndexPrefix;
    private final Logger logger;

    EsKeyCountBatchChecker(HttpClient client, String esHost, String esIndexPrefix, Logger logger) {
        this.client = client;
        this.esHost = esHost;
        this.esIndexPrefix = esIndexPrefix;
        this.logger = logger;
    }

    /** Returns key -> count for every key in the batch (0 for keys with no hits). */
    Map<String, Long> countBatch(List<String> batchKeys) throws Exception {
        return RetryExecutor.withRetry(() -> doCount(batchKeys), "ES_KEY_COUNT", logger);
    }

    private Map<String, Long> doCount(List<String> batchKeys) throws Exception {
        Map<String, Long> counts = new HashMap<>();
        for (String key : batchKeys) counts.put(key, 0L); // default every key to 0

        String indexPattern = indexPatternFor(batchKeys);
        List<Object> searchAfter = null;

        while (true) {
            String body = buildQuery(batchKeys, searchAfter);
            JsonNode root = post(indexPattern, body);

            JsonNode hits = root.path("hits").path("hits");
            if (!hits.isArray() || hits.isEmpty()) break;

            JsonNode lastSort = null;
            for (JsonNode hit : hits) {
                JsonNode keyNode = hit.path("_source").path("key");
                if (keyNode.isTextual()) {
                    counts.merge(keyNode.asText(), 1L, Long::sum);
                }
                lastSort = hit.path("sort");
            }

            if (hits.size() < PAGE_SIZE) break; // last page

            List<Object> next = new ArrayList<>();
            for (JsonNode s : lastSort) next.add(s.isTextual() ? s.asText() : s.asLong());
            searchAfter = next;
        }

        return counts;
    }

    private JsonNode post(String indexPattern, String body) throws Exception {
        String url = esHost + "/" + indexPattern + "/_search";
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("ES returned HTTP " + response.statusCode() + ": "
                + truncate(response.body()));
        }
        JsonNode root = MAPPER.readTree(response.body());
        if (root.has("error")) {
            throw new IllegalStateException("ES returned an error: " + truncate(root.get("error").toString()));
        }
        return root;
    }

    private String buildQuery(List<String> keys, List<Object> searchAfter) {
        StringBuilder termsArray = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) termsArray.append(',');
            termsArray.append('"').append(escape(keys.get(i))).append('"');
        }
        StringBuilder sb = new StringBuilder("{")
            .append("\"size\":").append(PAGE_SIZE).append(',')
            .append("\"_source\":[\"key\"],")
            .append("\"query\":{\"bool\":{\"filter\":[{\"terms\":{\"key\":[").append(termsArray).append("]}}]}},")
            .append("\"sort\":[\"_doc\"]"); // doc_values-free pagination order

        if (searchAfter != null) {
            sb.append(",\"search_after\":[");
            for (int i = 0; i < searchAfter.size(); i++) {
                if (i > 0) sb.append(',');
                Object v = searchAfter.get(i);
                sb.append(v instanceof String ? "\"" + v + "\"" : v.toString());
            }
            sb.append(']');
        }
        return sb.append('}').toString();
    }

    /**
     * Narrows the index target per batch from each key's own YYYY/MM date
     * prefix, same correctness-preserving optimization as EsBatchChecker
     * (com.smarsh.backlogger) - falls back to the fully open wildcard for
     * any key that doesn't parse as "YYYY/MM/...", since a generic key
     * list (unlike the S3-key reprocess file) isn't guaranteed to follow
     * that format at all.
     */
    private String indexPatternFor(List<String> keys) {
        Set<String> patterns = new LinkedHashSet<>();
        for (String key : keys) {
            String pattern = esIndexPrefix + "*";
            if (key.length() >= 7 && key.charAt(4) == '/' && key.charAt(7) == '/') {
                String year = key.substring(0, 4);
                String month = key.substring(5, 7);
                if (isDigits(year) && isDigits(month)) {
                    pattern = esIndexPrefix + year + "-" + month + "-*";
                }
            }
            patterns.add(pattern);
        }
        return String.join(",", patterns);
    }

    private static boolean isDigits(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) return false;
        }
        return true;
    }

    private static String escape(String key) {
        return key.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String truncate(String s) {
        return s.substring(0, Math.min(300, s.length()));
    }
}
