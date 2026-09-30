package com.smarsh.migration;

import com.smarsh.migration.QuadrimesterMonths.MonthWindow;
import com.smarsh.migration.QuadrimesterMonths.QuadKey;

import java.time.format.DateTimeFormatter;

/**
 * Pure string-building for every SQL/DDL statement this tool issues, plus
 * the table/path naming helpers. Every method takes already-resolved Java
 * values (no raw CLI strings without escaping).
 */
public class MigrationSql {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ── Naming helpers ─────────────────────────────────────────────────────

    static String sanitizedEntity(String entity) {
        return entity.replace(".", "_");
    }

    /** One persistent table per reporting entity - no year/quadrimester in the name. */
    static String tempTableName(String entity) {
        return "tier2_migration_" + sanitizedEntity(entity) + "_monthly_stage2_data";
    }

    /** One stable temp path per reporting entity - year/quadrimester/month are partition columns, not path segments. */
    static String tempPath(String bucket, String entity) {
        return "s3://" + bucket + "/temps/coc/reporting_entity=" + entity + "/_tmp/";
    }

    static String finalPath(String bucket, String entity, QuadKey key) {
        return "s3://" + bucket + "/eventlog_recon/tier2/migration/coc/reporting_entity=" + entity
            + "/MIGRATED/" + key.label() + "/";
    }

    // ── DDL ─────────────────────────────────────────────────────────────────

    static String createTableIfNotExists(String tableName, String tempPath) {
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + " ("
            + "source_id string, time_stamp string, key string, message_id string"
            + ") PARTITIONED BY (year string, quadrimester string, month string) "
            + "STORED AS PARQUET "
            + "LOCATION '" + tempPath + "'";
    }

    static String repairTable(String tableName) {
        return "MSCK REPAIR TABLE " + tableName;
    }

    // ── UNLOAD statements ──────────────────────────────────────────────────

    static String monthlyUnload(String stage2Table, String entity, QuadKey key, MonthWindow mw, String tempPath) {
        String quotedEntity = escape(entity);
        String quotedQuad = escape(key.label());
        return "UNLOAD ("
            + "SELECT source_id, time_stamp, key, message_id, "
            +   "'" + key.year() + "' AS year, '" + quotedQuad + "' AS quadrimester, '" + mw.monthLabel() + "' AS month "
            + "FROM ("
            +   "SELECT source_id, time_stamp, key, message_id, "
            +     "ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY time_stamp DESC) as rn "
            +   "FROM " + stage2Table + " "
            +   "WHERE reporting_entity = '" + quotedEntity + "' "
            +   "AND quadrimester = '" + quotedQuad + "' "
            +   "AND FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000) >= TIMESTAMP '" + mw.start().atStartOfDay().format(DATE_FMT) + "' "
            +   "AND FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000) < TIMESTAMP '" + mw.nextMonthStart().atStartOfDay().format(DATE_FMT) + "'"
            + ") t "
            + "WHERE rn = 1"
            + ") "
            + "TO '" + tempPath + "' "
            + "WITH (format = 'PARQUET', compression = 'SNAPPY', partitioned_by = ARRAY['year','quadrimester','month'])";
    }

    static String finalUnload(String tableName, QuadKey key, String finalPath) {
        return "UNLOAD ("
            + "SELECT source_id, time_stamp, key, message_id "
            + "FROM " + tableName + " WHERE quadrimester = '" + escape(key.label()) + "'"
            + ") "
            + "TO '" + finalPath + "' "
            + "WITH (format = 'PARQUET', compression = 'SNAPPY')";
    }

    // ── Verification ────────────────────────────────────────────────────────

    static String verifyTempCounts(String tableName, QuadKey key) {
        return "SELECT COUNT(*) AS total_rows, COUNT(DISTINCT source_id) AS distinct_sources "
            + "FROM " + tableName + " WHERE quadrimester = '" + escape(key.label()) + "'";
    }

    static String verifyStageCounts(String stage2Table, String entity, QuadKey key) {
        return "SELECT COUNT(*) AS total_rows, COUNT(DISTINCT source_id) AS distinct_sources "
            + "FROM " + stage2Table + " "
            + "WHERE reporting_entity = '" + escape(entity) + "' AND quadrimester = '" + escape(key.label()) + "'";
    }

    private static String escape(String s) {
        return s.replace("'", "''");
    }
}
