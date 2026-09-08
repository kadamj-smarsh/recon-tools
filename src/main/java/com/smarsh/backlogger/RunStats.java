package com.smarsh.backlogger;

import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe counters for the final run summary. */
public class RunStats {

    final AtomicLong keysRead = new AtomicLong();
    final AtomicLong keysSkippedFromResume = new AtomicLong();
    final AtomicLong batchesProcessed = new AtomicLong();
    final AtomicLong keysAlreadyInEs = new AtomicLong();
    final AtomicLong keysSubmitted = new AtomicLong();
    final AtomicLong batchesFailed = new AtomicLong();
    final AtomicLong keysFailed = new AtomicLong();

    String renderSummary(long elapsedMillis) {
        double elapsedSec = elapsedMillis / 1000.0;
        return """
            ==== Run Summary ====
            Keys read from input CSV      : %d
            Keys skipped (already handled in a prior run) : %d
            Batches processed             : %d
            Keys already in ES (skipped)  : %d
            Keys submitted to backlogger  : %d
            Batches failed (all retries exhausted) : %d
            Keys in failed batches        : %d
            Elapsed time                  : %.1fs
            ======================
            """.formatted(
                keysRead.get(),
                keysSkippedFromResume.get(),
                batchesProcessed.get(),
                keysAlreadyInEs.get(),
                keysSubmitted.get(),
                batchesFailed.get(),
                keysFailed.get(),
                elapsedSec);
    }
}
