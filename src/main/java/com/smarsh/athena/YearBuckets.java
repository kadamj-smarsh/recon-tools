package com.smarsh.athena;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the list of "year buckets" to query, derived from the fixed
 * quadrimester master list:
 *
 *   pre-1970, 1970-1999, 2000-T1..2000-T3, ..., 2026-T1..2026-T3, post-2026
 *
 * A calendar year (2000-2026) maps to its three quadrimesters
 * (YYYY-T1/T2/T3); the three special multi-year buckets (pre-1970,
 * 1970-1999, post-2026) map to themselves as a single quadrimester value.
 * Nothing here is hardcoded as "the only years that matter" - --years lets
 * a run target a subset instead of the full range.
 */
public class YearBuckets {

    static final int FIRST_YEAR = 2000;
    static final int LAST_YEAR = 2026;
    static final List<String> SPECIAL_BUCKETS = List.of("pre-1970", "1970-1999", "post-2026");

    record Bucket(String label, List<String> quadrimesterValues) {}

    /** Full default range: every calendar year 2000-2026 plus the three special buckets. */
    static List<Bucket> fullRange() {
        List<Bucket> buckets = new ArrayList<>();
        for (int year = FIRST_YEAR; year <= LAST_YEAR; year++) {
            buckets.add(forYearLabel(String.valueOf(year)));
        }
        for (String special : SPECIAL_BUCKETS) {
            buckets.add(forYearLabel(special));
        }
        return buckets;
    }

    /** Parses a comma-separated --years value (e.g. "2023" or "2020,2021,pre-1970") into buckets. */
    static List<Bucket> parse(String yearsArg) {
        List<Bucket> buckets = new ArrayList<>();
        for (String token : yearsArg.split(",")) {
            String label = token.trim();
            if (!label.isEmpty()) buckets.add(forYearLabel(label));
        }
        return buckets;
    }

    private static Bucket forYearLabel(String label) {
        if (SPECIAL_BUCKETS.contains(label)) {
            return new Bucket(label, List.of(label));
        }
        if (label.matches("\\d{4}") ) {
            return new Bucket(label, List.of(label + "-T1", label + "-T2", label + "-T3"));
        }
        throw new IllegalArgumentException(
            "Unrecognized year/bucket \"" + label + "\" - expected a 4-digit year (e.g. 2023) or one of "
                + SPECIAL_BUCKETS);
    }
}
