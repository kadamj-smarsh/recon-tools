package com.smarsh.sourcelookup;

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
import java.util.List;

/**
 * Finds the ES "key" field value(s) for a given sourceId, using the same
 * query_string phrase match on text.sys.content as
 * com.smarsh.backlogger.DuplicateSourceIdChecker (sourceId is only ever
 * present in free text, not a discrete keyword field - confirmed
 * previously), plus a wide startTime range filter.
 *
 * Same _count-first optimization: a cheap _count tells us how many docs
 * match before paying for a _search that actually returns _source. Most
 * sourceIds are expected to resolve to exactly one key.
 */
public class EsKeyResolver {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DATE_FROM = "2008-01-01T00:00:00.000+0000";
    private static final String DATE_TO = "2027-12-31T23:59:59.999+0000";

    private final HttpClient client;
    private final String esHost;
    private final String esIndexPrefix;
    private final Logger logger;

    EsKeyResolver(HttpClient client, String esHost, String esIndexPrefix, Logger logger) {
        this.client = client;
        this.esHost = esHost;
        this.esIndexPrefix = esIndexPrefix;
        this.logger = logger;
    }

    /** Returns every "key" value found for this sourceId (usually 0 or 1, occasionally more for known duplicates). */
    List<String> findKeys(String sourceId) throws Exception {
        return RetryExecutor.withRetry(() -> doFindKeys(sourceId), "ES_KEY_RESOLVE", logger);
    }

    private List<String> doFindKeys(String sourceId) throws Exception {
        long count = count(sourceId);
        if (count == 0) return List.of();

        String url = esHost + "/" + esIndexPrefix + "*/_search";
        String body = "{"
            + "\"size\":" + Math.min(count, 1000) + ","
            + "\"_source\":[\"key\"],"
            + "\"query\":" + queryClause(sourceId)
            + "}";

        JsonNode root = post(url, body);
        List<String> keys = new ArrayList<>();
        JsonNode hits = root.path("hits").path("hits");
        if (hits.isArray()) {
            for (JsonNode hit : hits) {
                JsonNode keyNode = hit.path("_source").path("key");
                if (keyNode.isTextual()) keys.add(keyNode.asText());
            }
        }
        return keys;
    }

    private long count(String sourceId) throws Exception {
        String url = esHost + "/" + esIndexPrefix + "*/_count";
        String body = "{\"query\":" + queryClause(sourceId) + "}";
        JsonNode root = post(url, body);
        return root.path("count").asLong(0);
    }

    private String queryClause(String sourceId) {
        String phrase = escape("X-SMARSH-SOURCE-ID:" + sourceId);
        return "{\"bool\":{\"must\":["
            + "{\"query_string\":{\"query\":\"\\\"" + phrase + "\\\"\",\"fields\":[\"text.sys.content^1.0\"]}},"
            + "{\"range\":{\"startTime\":{\"gte\":\"" + DATE_FROM + "\",\"lte\":\"" + DATE_TO + "\"}}}"
            + "]}}";
    }

    private JsonNode post(String url, String body) throws Exception {
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

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.substring(0, Math.min(300, s.length()));
    }
}
