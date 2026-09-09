/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HttpPullPollUrlResolverTest {

    @Test
    void blankOverrideUsesProfileUrl() {
        assertThat(HttpPullPollUrlResolver.resolve("http://profile/api/data", null))
                .isEqualTo("http://profile/api/data");
        assertThat(HttpPullPollUrlResolver.resolve("http://profile/api/data", "  "))
                .isEqualTo("http://profile/api/data");
    }

    @Test
    void hostPortOverrideKeepsProfilePathAndQuery() {
        assertThat(HttpPullPollUrlResolver.resolve("http://profile.example:80/api/list?q=1", "10.0.0.8:8080"))
                .isEqualTo("http://10.0.0.8:8080/api/list?q=1");
    }

    @Test
    void fullUrlOverrideReplacesProfileUrl() {
        assertThat(HttpPullPollUrlResolver.resolve("http://profile/api/data", "https://other/v2/metrics"))
                .isEqualTo("https://other/v2/metrics");
    }
}
