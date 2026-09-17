package com.smarsh.reconcile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs the ES side of the reconciliation: date_histogram aggregations (one
 * per drill-down level) and, at leaf level, the actual key fetch - with
 * search_after pagination for the rare case where a bucket's count is
 * still >= 10000 even at the deepest (second) level.
 */
public class EsHistogramClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int PAGE_SIZE = 10000; // ES's max result window

    private final HttpClient client;
    private final String esHost;
    private final String esIndexPrefix;
    private final String reportingEntity;

    EsHistogramClient(HttpClient client, String esHost, String esIndexPrefix, String reportingEntity) {
        this.client = client;
        this.esHost = esHost;
        this.esIndexPrefix = esIndexPrefix;
        this.reportingEntity = reportingEntity;
    }

    /**
     * Runs a date_histogram over the window at the given calendar interval,
     * returning bucket key (as ES formatted it) -> doc_count.
     */
    Map<String, Long> histogram(TimeWindow window, String calendarInterval, String dateFormat, String indexPattern) throws Exception {
        String body = "{"
            + "\"size\":0,"
            + "\"query\":{\"bool\":{"
            +   "\"must\":[{\"query_string\":{\"query\":\"" + queryStringPhrase() + "\",\"fields\":[\"text.sys.content^1.0\"]}}],"
            +   "\"filter\":[{\"range\":{\"startTime\":{\"gte\":\"" + window.esGte() + "\",\"lte\":\"" + window.esLte() + "\"}}}]"
            + "}},"
            + "\"aggs\":{\"group_by_year\":{\"date_histogram\":{"
            +   "\"field\":\"startTime\",\"calendar_interval\":\"" + calendarInterval + "\","
            +   "\"format\":\"" + dateFormat + "\",\"min_doc_count\":1"
            + "}}}"
            + "}";

        JsonNode root = post(indexPattern + "/_search", body);

        Map<String, Long> result = new LinkedHashMap<>();
        JsonNode buckets = root.path("aggregations").path("group_by_year").path("buckets");
        if (buckets.isArray()) {
            for (JsonNode bucket : buckets) {
                result.put(bucket.path("key_as_string").asText(), bucket.path("doc_count").asLong());
            }
        }
        return result;
    }

    /**
     * Fetches every key in the window. Uses a single request when count <
     * PAGE_SIZE (the common case, decided by the caller); pages via
     * search_after otherwise, so a leaf bucket that's still >= 10000 even
     * at second-level granularity is still fetched completely and
     * correctly, not silently truncated.
     */
    List<String> fetchKeys(TimeWindow window, String indexPattern, boolean paginate) throws Exception {
        List<String> keys = new ArrayList<>();
        List<Object> searchAfter = null;

        while (true) {
            String body = buildLeafQuery(window, searchAfter);
            JsonNode root = post(indexPattern + "/_search", body);

            JsonNode hits = root.path("hits").path("hits");
            if (!hits.isArray() || hits.isEmpty()) break;

            JsonNode lastSort = null;
            for (JsonNode hit : hits) {
                JsonNode keyNode = hit.path("_source").path("key");
                if (keyNode.isTextual()) keys.add(keyNode.asText());
                lastSort = hit.path("sort");
            }

            if (!paginate || hits.size() < PAGE_SIZE) break;

            List<Object> next = new ArrayList<>();
            for (JsonNode s : lastSort) next.add(s.isTextual() ? s.asText() : s.asLong());
            searchAfter = next;
        }
        return keys;
    }

    private String buildLeafQuery(TimeWindow window, List<Object> searchAfter) {
        StringBuilder sb = new StringBuilder("{")
            .append("\"size\":").append(PAGE_SIZE).append(',')
            .append("\"_source\":[\"key\"],")
            .append("\"query\":{\"bool\":{")
            .append("\"must\":[{\"query_string\":{\"query\":\"").append(queryStringPhrase()).append("\",\"fields\":[\"text.sys.content^1.0\"]}}],")
            .append("\"filter\":[{\"range\":{\"startTime\":{\"gte\":\"").append(window.esGte())
            .append("\",\"lte\":\"").append(window.esLte()).append("\"}}}]")
            .append("}},")
            .append("\"sort\":[{\"startTime\":\"asc\"},{\"_id\":\"asc\"}]");
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

    private String queryStringPhrase() {
        String phrase = "X-SMARSH-REPORTING-ENTITY: " + reportingEntity;
        return "\\\"" + phrase.replace("\\", "\\\\").replace("\"", "\\\"") + "\\\"";
    }

    private static final int MAX_ATTEMPTS = 3;
    private static final long[] BACKOFF_MS = {2000, 5000};

    private JsonNode post(String path, String body) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return doPost(path, body);
            } catch (Exception e) {
                last = e;
                System.err.printf("[ES] attempt %d/%d failed for %s: %s%n", attempt, MAX_ATTEMPTS, path, e.getMessage());
                if (attempt < MAX_ATTEMPTS) Thread.sleep(BACKOFF_MS[attempt - 1]);
            }
        }
        throw last;
    }

    private JsonNode doPost(String path, String body) throws Exception {
        String url = esHost + "/" + path;
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

    String indexPatternFor(int year) {
        return esIndexPrefix + year + "-*";
    }

    private static String truncate(String s) {
        return s.substring(0, Math.min(300, s.length()));
    }
}
