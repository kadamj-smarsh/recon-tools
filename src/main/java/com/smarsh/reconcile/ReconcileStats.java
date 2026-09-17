package com.smarsh.reconcile;

import java.util.concurrent.atomic.AtomicLong;

/** Thread-safe-ish counters (not actually contended, this tool runs sequentially) for the final summary. */
public class ReconcileStats {
    final AtomicLong mismatchedMonths = new AtomicLong();
    final AtomicLong mismatchedDays = new AtomicLong();
    final AtomicLong mismatchedHours = new AtomicLong();
    final AtomicLong mismatchedMinutes = new AtomicLong();
    final AtomicLong mismatchedSeconds = new AtomicLong();
    final AtomicLong esOnlyKeys = new AtomicLong();
    final AtomicLong athenaOnlyKeys = new AtomicLong();

    String render() {
        return """
            ==== Reconciliation Summary ====
            Mismatched months  : %d
            Mismatched days    : %d
            Mismatched hours   : %d
            Mismatched minutes : %d
            Mismatched seconds : %d
            ES-only keys found : %d
            Athena-only keys found : %d
            =================================
            """.formatted(mismatchedMonths.get(), mismatchedDays.get(), mismatchedHours.get(),
                mismatchedMinutes.get(), mismatchedSeconds.get(), esOnlyKeys.get(), athenaOnlyKeys.get());
    }
}
