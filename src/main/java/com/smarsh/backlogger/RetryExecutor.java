package com.smarsh.backlogger;

import java.util.concurrent.Callable;

/** Generic retry-with-backoff helper shared by the ES and backlogger calls. */
public class RetryExecutor {

    static final int MAX_ATTEMPTS = 3;
    static final long[] BACKOFF_MS = {1000, 3000, 9000};

    /**
     * Runs {@code action} up to MAX_ATTEMPTS times. Any exception thrown by
     * action (including one deliberately thrown for a "logical" failure,
     * e.g. an ES error field or a backlogger response without "submitted")
     * triggers a retry with backoff. If all attempts fail, the last
     * exception is rethrown, wrapped in BatchFailedException by the caller.
     */
    static <T> T withRetry(Callable<T> action, String opLabel, Logger logger) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return action.call();
            } catch (Exception e) {
                last = e;
                logger.warn(String.format("[%s] attempt %d/%d failed: %s",
                    opLabel, attempt, MAX_ATTEMPTS, e.getMessage()));
                if (attempt < MAX_ATTEMPTS) {
                    Thread.sleep(BACKOFF_MS[attempt - 1]);
                }
            }
        }
        throw last;
    }
}
