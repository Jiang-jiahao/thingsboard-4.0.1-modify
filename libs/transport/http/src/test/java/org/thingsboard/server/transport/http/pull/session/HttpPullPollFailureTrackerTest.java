/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull.session;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class HttpPullPollFailureTrackerTest {

    private static final long MINUTE = TimeUnit.MINUTES.toMillis(1);

    @Test
    void firstFailureIsReported() {
        HttpPullPollFailureTracker tracker = new HttpPullPollFailureTracker();

        HttpPullPollFailureTracker.Report report = tracker.onFailure("HTTP status 500", 0);

        assertThat(report.reported()).isTrue();
        assertThat(report.firstOrChanged()).isTrue();
        assertThat(report.consecutive()).isEqualTo(1);
    }

    @Test
    void repeatedSameFailureIsSuppressedUntilInterval() {
        HttpPullPollFailureTracker tracker = new HttpPullPollFailureTracker();
        tracker.onFailure("HTTP status 500", 0);

        // 轮询间隔内连失败多次：不再输出
        HttpPullPollFailureTracker.Report last = null;
        for (int i = 1; i <= 30; i++) {
            last = tracker.onFailure("HTTP status 500", i * 5000L);
            assertThat(last.reported()).isFalse();
            assertThat(last.firstOrChanged()).isFalse();
        }
        assertThat(last.consecutive()).isEqualTo(31);
    }

    @Test
    void sameFailureInNewIntervalIsReportedAsSummary() {
        HttpPullPollFailureTracker tracker = new HttpPullPollFailureTracker();
        tracker.onFailure("HTTP status 500", 0);

        HttpPullPollFailureTracker.Report report = tracker.onFailure("HTTP status 500", 6 * MINUTE);

        assertThat(report.reported()).isTrue();
        assertThat(report.firstOrChanged()).isFalse();
        assertThat(report.consecutive()).isEqualTo(2);
    }

    @Test
    void changedFailureIsReportedAsFirst() {
        HttpPullPollFailureTracker tracker = new HttpPullPollFailureTracker();
        tracker.onFailure("HTTP status 500", 0);

        HttpPullPollFailureTracker.Report report = tracker.onFailure("ConnectException: Connection refused", MINUTE);

        assertThat(report.reported()).isTrue();
        assertThat(report.firstOrChanged()).isTrue();
        assertThat(report.consecutive()).isEqualTo(2);
    }

    @Test
    void resetReturnsCountAndMakesNextFailureFirstAgain() {
        HttpPullPollFailureTracker tracker = new HttpPullPollFailureTracker();
        tracker.onFailure("HTTP status 500", 0);
        tracker.onFailure("HTTP status 500", 5000);

        assertThat(tracker.reset()).isEqualTo(2);
        assertThat(tracker.reset()).isZero();

        HttpPullPollFailureTracker.Report report = tracker.onFailure("HTTP status 500", 10_000);
        assertThat(report.firstOrChanged()).isTrue();
        assertThat(report.consecutive()).isEqualTo(1);
    }
}
