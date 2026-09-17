package com.smarsh.athena;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs one Athena query end-to-end: StartQueryExecution -> poll
 * GetQueryExecution until SUCCEEDED/FAILED/CANCELLED -> GetQueryResults.
 *
 * ResultConfiguration is only set when an explicit S3 output location is
 * given (--s3-output-location) - Athena requires an S3 staging location for
 * every query regardless of whether we set it ourselves or the workgroup
 * has a default configured, but this tool never reads or manages that S3
 * object itself either way, it only ever calls GetQueryResults and writes
 * the returned rows to a local CSV.
 */
public class AthenaQueryRunner {

    private final AthenaClient client;
    private final String database;
    private final String workgroup;
    private final String s3OutputLocation; // nullable - omitted if workgroup has its own default

    public AthenaQueryRunner(AthenaClient client, String database, String workgroup, String s3OutputLocation) {
        this.client = client;
        this.database = database;
        this.workgroup = workgroup;
        this.s3OutputLocation = s3OutputLocation;
    }

    /** Returns each result row as a List<String> of column values, header row excluded. */
    public List<List<String>> runQuery(String sql) throws Exception {
        StartQueryExecutionRequest.Builder startRequestBuilder = StartQueryExecutionRequest.builder()
            .queryString(sql)
            .queryExecutionContext(QueryExecutionContext.builder().database(database).build())
            .workGroup(workgroup);

        if (s3OutputLocation != null && !s3OutputLocation.isBlank()) {
            startRequestBuilder.resultConfiguration(
                ResultConfiguration.builder().outputLocation(s3OutputLocation).build());
        }

        StartQueryExecutionRequest startRequest = startRequestBuilder.build();

        StartQueryExecutionResponse startResponse = client.startQueryExecution(startRequest);
        String queryExecutionId = startResponse.queryExecutionId();

        pollUntilDone(queryExecutionId);

        return fetchResults(queryExecutionId);
    }

    private void pollUntilDone(String queryExecutionId) throws Exception {
        GetQueryExecutionRequest request = GetQueryExecutionRequest.builder()
            .queryExecutionId(queryExecutionId)
            .build();

        while (true) {
            GetQueryExecutionResponse response = client.getQueryExecution(request);
            QueryExecutionState state = response.queryExecution().status().state();

            if (state == QueryExecutionState.SUCCEEDED) {
                return;
            }
            if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED) {
                String reason = response.queryExecution().status().stateChangeReason();
                throw new IllegalStateException("Athena query " + state + ": " + reason);
            }
            // QUEUED or RUNNING - wait and poll again.
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        }
    }

    private List<List<String>> fetchResults(String queryExecutionId) {
        GetQueryResultsRequest request = GetQueryResultsRequest.builder()
            .queryExecutionId(queryExecutionId)
            .build();

        List<List<String>> rows = new ArrayList<>();
        boolean first = true;
        var iterable = client.getQueryResultsPaginator(request);
        for (var page : iterable) {
            for (Row row : page.resultSet().rows()) {
                if (first) {
                    // GetQueryResults' first row across the whole result set is
                    // the column header line, not data - skip it exactly once.
                    first = false;
                    continue;
                }
                List<String> values = new ArrayList<>();
                row.data().forEach(datum -> values.add(datum.varCharValue()));
                rows.add(values);
            }
        }
        return rows;
    }
}
