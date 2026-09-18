package org.thingsboard.server.common.data.queue;

public enum ProcessingStrategyType {
    SKIP_ALL_FAILURES, SKIP_ALL_FAILURES_AND_TIMED_OUT, RETRY_ALL, RETRY_FAILED, RETRY_TIMED_OUT, RETRY_FAILED_AND_TIMED_OUT
}