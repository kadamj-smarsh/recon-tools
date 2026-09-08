package com.smarsh.backlogger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Checks a batch of keys against Elasticsearch using a terms query on
 * "key". Runs a cheap _count first; only escalates to the more expensive
 * _search (needed to find out exactly which keys matched) when the count
 * is non-zero. Determines which keys in the batch already exist (matched)
 * vs are missing.
 *
 * The index target is derived per-batch from each key's own date prefix
 * (keys look like "YYYY/MM/DD/..."), narrowing to e.g. "rmaas-tier2-2017-07-*"
 * instead of the fully open "rmaas-tier2-*". This is a correctness-preserving
 * optimization, not a hardcoded date range: a batch with keys from multiple
 * months queries multiple narrow index patterns (comma-joined) rather than
 * ever falling back to scanning every index. Narrowing matters because the
 * open wildcard fans every query out across every shard in the cluster's
 * whole history, which was tripping Elasticsearch's parent circuit breaker
 * under concurrency (see run.log from the --concurrency 10 test) - the
 * per-query "size" isn't the dominant cost, the shard fan-out is.
 */
public class EsBatchChecker {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient client;
    private final String esHost;
    private final Logger logger;

    EsBatchChecker(HttpClient client, String esHost, Logger logger) {
        this.client = client;
        this.esHost = esHost;
        this.logger = logger;
    }

    /**
     * Returns the subset of batchKeys that are NOT already present in ES.
     *
     * First runs a cheap _count with the same terms query: if zero keys in
     * the batch match anything, every key is missing and there's no need to
     * pay for a _search's hit-fetch-and-merge just to learn that. Only when
     * count > 0 do we run the full _search to figure out exactly WHICH keys
     * matched (a plain count can't tell us that). For a "keys to reprocess"
     * list, most batches are expected to be entirely missing, so this should
     * skip the more expensive call for the common case.
     */
    List<String> findMissingKeys(List<String> batchKeys) throws BatchFailedException {
        long matchCount;
        try {
            matchCount = RetryExecutor.withRetry(() -> doCount(batchKeys), "ES_COUNT", logger);
        } catch (Exception e) {
            throw new BatchFailedException("ES_COUNT", batchKeys, e);
        }

        if (matchCount == 0) {
            return batchKeys; // nothing in this batch exists in ES - all missing
        }

        try {
            return RetryExecutor.withRetry(() -> doCheck(batchKeys), "ES_CHECK", logger);
        } catch (Exception e) {
            throw new BatchFailedException("ES_CHECK", batchKeys, e);
        }
    }

    private long doCount(List<String> batchKeys) throws Exception {
        String url = esHost + "/" + indexPatternFor(batchKeys) + "/_count";
        String body = buildCountQuery(batchKeys);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IllegalStateException("ES _count returned HTTP " + response.statusCode() + ": "
                + truncate(response.body()));
        }

        JsonNode root = MAPPER.readTree(response.body());
        if (root.has("error")) {
            throw new IllegalStateException("ES _count returned an error: " + truncate(root.get("error").toString()));
        }

        return root.path("count").asLong(0);
    }

    private List<String> doCheck(List<String> batchKeys) throws Exception {
        String url = esHost + "/" + indexPatternFor(batchKeys) + "/_search";
        String body = buildQuery(batchKeys);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(body))
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

        Set<String> matchedKeys = new HashSet<>();
        JsonNode hits = root.path("hits").path("hits");
        if (hits.isArray()) {
            for (JsonNode hit : hits) {
                JsonNode keyNode = hit.path("_source").path("key");
                if (!keyNode.isMissingNode() && keyNode.isTextual()) {
                    matchedKeys.add(keyNode.asText());
                }
            }
        }

        List<String> missing = new ArrayList<>();
        for (String key : batchKeys) {
            if (!matchedKeys.contains(key)) missing.add(key);
        }
        return missing;
    }

    private static String buildQuery(List<String> keys) {
        StringBuilder termsArray = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) termsArray.append(',');
            termsArray.append('"').append(escape(keys.get(i))).append('"');
        }
        // size = exactly the number of keys in this batch. A key can match at
        // most one document (re-indexing overwrites, it doesn't duplicate),
        // so this is never truncated - and it's far cheaper for ES's
        // coordinating node to merge than the previous fixed size:10000,
        // which was tripping the parent circuit breaker under concurrency
        // (rmaas-tier2-* spans many shards; a bigger "size" multiplies the
        // per-query reduce-phase memory cost across all of them).
        return "{"
            + "\"size\":" + keys.size() + ","
            + "\"_source\":[\"key\"],"
            + "\"query\":{\"bool\":{\"must\":[{\"terms\":{\"key\":[" + termsArray + "]}}]}}"
            + "}";
    }

    private static String buildCountQuery(List<String> keys) {
        StringBuilder termsArray = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) termsArray.append(',');
            termsArray.append('"').append(escape(keys.get(i))).append('"');
        }
        // _count takes no "size"/"_source" - it only ever returns a number.
        return "{\"query\":{\"bool\":{\"must\":[{\"terms\":{\"key\":[" + termsArray + "]}}]}}}";
    }

    /**
     * Builds a comma-joined list of month-scoped index patterns covering
     * every key in the batch, e.g. "rmaas-tier2-2017-07-*" - or
     * "rmaas-tier2-2017-07-*,rmaas-tier2-2017-08-*" if the batch happens to
     * span two months. Falls back to the fully open "rmaas-tier2-*" only for
     * a key that doesn't parse as "YYYY/MM/..." (so nothing is ever silently
     * excluded from the search).
     */
    private static String indexPatternFor(List<String> keys) {
        Set<String> patterns = new LinkedHashSet<>();
        for (String key : keys) {
            String pattern = "rmaas-tier2-*";
            if (key.length() >= 7 && key.charAt(4) == '/' && key.charAt(7) == '/') {
                String year = key.substring(0, 4);
                String month = key.substring(5, 7);
                if (isDigits(year) && isDigits(month)) {
                    pattern = "rmaas-tier2-" + year + "-" + month + "-*";
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
        return s.substring(0, Math.min(200, s.length()));
    }
}
