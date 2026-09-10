package com.smarsh.backlogger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * Fetches and caches an OAuth2 client_credentials access token from the UAA
 * token endpoint, e.g.:
 *
 *   POST {tokenUrl}
 *   content-type: application/x-www-form-urlencoded
 *   grant_type=client_credentials&client_id={clientId}&client_secret={clientSecret}
 *
 * The token is cached in memory and reused across every ES/backlogger call
 * for the whole run - it is NOT re-fetched per request. A fresh token is
 * only fetched on first use and after invalidate() is called (e.g. when a
 * downstream call gets HTTP 401, meaning the cached token has expired or
 * been revoked).
 */
public class TokenProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient client;
    private final String tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final Logger logger;

    private volatile String cachedToken;
    private volatile Instant expiresAt = Instant.EPOCH;

    TokenProvider(HttpClient client, String tokenUrl, String clientId, String clientSecret, Logger logger) {
        this.client = client;
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.logger = logger;
    }

    /** Returns a cached token if still valid, otherwise fetches a new one. */
    synchronized String getToken() throws Exception {
        if (cachedToken == null || Instant.now().isAfter(expiresAt)) {
            fetchNewToken();
        }
        return cachedToken;
    }

    /** Forces the next getToken() call to fetch a fresh token (e.g. after an HTTP 401). */
    synchronized void invalidate() {
        cachedToken = null;
        expiresAt = Instant.EPOCH;
    }

    private void fetchNewToken() throws Exception {
        String body = "grant_type=client_credentials"
            + "&client_id=" + encode(clientId)
            + "&client_secret=" + encode(clientSecret);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(tokenUrl))
            .header("content-type", "application/x-www-form-urlencoded")
            .timeout(Duration.ofSeconds(30))
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IllegalStateException("Failed to fetch token from " + tokenUrl + ", HTTP "
                + response.statusCode() + ": " + truncate(response.body()));
        }

        JsonNode root = MAPPER.readTree(response.body());
        JsonNode tokenNode = root.path("access_token");
        if (!tokenNode.isTextual() || tokenNode.asText().isBlank()) {
            throw new IllegalStateException("Token response had no access_token field: " + truncate(response.body()));
        }

        cachedToken = tokenNode.asText();

        // expires_in is in seconds; leave a 60s safety margin so we refresh
        // slightly before the token actually expires, not right at the edge.
        long expiresInSeconds = root.path("expires_in").asLong(3600);
        expiresAt = Instant.now().plusSeconds(Math.max(60, expiresInSeconds - 60));

        logger.info("Fetched new backlogger access token (expires in ~" + expiresInSeconds + "s).");
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.substring(0, Math.min(200, s.length()));
    }
}
