package com.smarsh.backlogger;

import java.util.List;

/** Thrown when a batch exhausts all retry attempts for a given stage. */
public class BatchFailedException extends Exception {

    final String stage; // "ES_CHECK" or "BACKLOGGER_SUBMIT"
    final List<String> keys;

    BatchFailedException(String stage, List<String> keys, Throwable cause) {
        super(cause.getMessage(), cause);
        this.stage = stage;
        this.keys = keys;
    }
}
