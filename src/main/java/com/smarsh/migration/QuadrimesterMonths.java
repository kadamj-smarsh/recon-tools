package com.smarsh.migration;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure calendar utility, no I/O: year -> quadrimester -> the 4 constituent
 * months' date boundaries. T1=Jan-Apr, T2=May-Aug, T3=Sep-Dec (confirmed
 * mapping, same as com.smarsh.reconcile.TimeWindow / com.smarsh.sourcelookup.Quadrimester).
 */
public class QuadrimesterMonths {

    record QuadKey(int year, int quadNum) {
        String label() {
            return year + "-T" + quadNum;
        }
    }

    record MonthWindow(int year, int monthNum, String monthLabel, LocalDate start, LocalDate nextMonthStart) {}

    /** The 4 months of a quadrimester, in order. */
    static List<MonthWindow> monthsOf(QuadKey key) {
        int firstMonth = (key.quadNum() - 1) * 4 + 1; // T1->1, T2->5, T3->9
        List<MonthWindow> months = new ArrayList<>(4);
        for (int m = firstMonth; m < firstMonth + 4; m++) {
            LocalDate start = LocalDate.of(key.year(), m, 1);
            months.add(new MonthWindow(key.year(), m, String.format("%02d", m), start, start.plusMonths(1)));
        }
        return months;
    }

    /** All 3 quadrimesters (T1,T2,T3) for every year in the list, in order. */
    static List<QuadKey> expand(List<Integer> years) {
        List<QuadKey> keys = new ArrayList<>();
        for (int year : years) {
            for (int q = 1; q <= 3; q++) keys.add(new QuadKey(year, q));
        }
        return keys;
    }

    /** Parses "2025-T3" back into a QuadKey. Throws IllegalArgumentException on anything else. */
    static QuadKey parseLabel(String label) {
        String[] parts = label.split("-T");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid quadrimester label: \"" + label + "\" (expected e.g. \"2025-T3\")");
        }
        try {
            int year = Integer.parseInt(parts[0]);
            int quad = Integer.parseInt(parts[1]);
            if (quad < 1 || quad > 3) throw new NumberFormatException();
            return new QuadKey(year, quad);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid quadrimester label: \"" + label + "\" (expected e.g. \"2025-T3\")");
        }
    }
}
