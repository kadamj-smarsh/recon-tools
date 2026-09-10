package com.smarsh.backlogger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * Submits a batch of missing keys to the backlogger replayKeys API.
 *
 * Success is judged ONLY on the HTTP call itself: HTTP 2xx AND a response
 * body containing "submitted". The API gives no other useful detail on
 * success, and actual re-indexing happens later/asynchronously depending on
 * backlogger's own queue depth — that is out of scope for this tool to
 * track, and is NOT treated as a failure/retry condition.
 *
 * The bearer token comes from a shared TokenProvider (fetched once via
 * OAuth client_credentials, cached, reused across every call). On an HTTP
 * 401 the cached token is invalidated so the next retry attempt fetches a
 * fresh one instead of repeating the same stale token three times.
 */
public class BackloggerClient {

    private final HttpClient client;
    private final String url;
    private final TokenProvider tokenProvider;
    private final Logger logger;

    BackloggerClient(HttpClient client, String url, TokenProvider tokenProvider, Logger logger) {
        this.client = client;
        this.url = url;
        this.tokenProvider = tokenProvider;
        this.logger = logger;
    }

    /** Submits the batch. No-op if keys is empty. */
    void submit(List<String> keys) throws BatchFailedException {
        if (keys.isEmpty()) return;
        try {
            RetryExecutor.withRetry(() -> doSubmit(keys), "BACKLOGGER_SUBMIT", logger);
        } catch (Exception e) {
            throw new BatchFailedException("BACKLOGGER_SUBMIT", keys, e);
        }
    }

    private Void doSubmit(List<String> keys) throws Exception {
        String body = String.join(",", keys);
        String token = tokenProvider.getToken();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("authorization", "Bearer " + token)
            .header("content-type", "application/json")
            .timeout(Duration.ofSeconds(60))
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 401) {
            // Cached token expired/revoked - invalidate so the next retry
            // attempt (RetryExecutor) fetches a fresh one instead of
            // repeating the same stale token.
            tokenProvider.invalidate();
            throw new IllegalStateException("Backlogger call failed, HTTP 401 (token expired/invalid) - "
                + "will fetch a fresh token on retry: " + truncate(response.body()));
        }

        boolean ok = response.statusCode() >= 200 && response.statusCode() < 300
            && response.body() != null && response.body().toLowerCase().contains("submitted");

        if (!ok) {
            throw new IllegalStateException("Backlogger call failed, HTTP " + response.statusCode()
                + ": " + truncate(response.body()));
        }
        return null;
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.substring(0, Math.min(200, s.length()));
    }
}
