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

    @Test
    void pathOnlyProfileUrlIsMergedWithDeviceOrigin() {
        assertThat(HttpPullPollUrlResolver.resolve("/api/list?q=1", "10.0.0.8:8080"))
                .isEqualTo("http://10.0.0.8:8080/api/list?q=1");
    }

    @Test
    void pathWithoutLeadingSlashIsNormalized() {
        assertThat(HttpPullPollUrlResolver.resolve("api/data", "10.0.0.8:8080"))
                .isEqualTo("http://10.0.0.8:8080/api/data");
    }

    @Test
    void schemeAndHostComeFromDeviceOverride() {
        assertThat(HttpPullPollUrlResolver.resolve("/api/data", "https://api.example.com"))
                .isEqualTo("https://api.example.com/api/data");
    }

    @Test
    void isAbsoluteRejectsRelativeOrHostlessUrls() {
        assertThat(HttpPullPollUrlResolver.isAbsolute("http://10.0.0.8:8080/api/data")).isTrue();
        assertThat(HttpPullPollUrlResolver.isAbsolute("https://api.example.com/api/data")).isTrue();
        assertThat(HttpPullPollUrlResolver.isAbsolute("/api/data")).isFalse();
        assertThat(HttpPullPollUrlResolver.isAbsolute("api/data")).isFalse();
        assertThat(HttpPullPollUrlResolver.isAbsolute("10.0.0.8:8080")).isFalse();
        assertThat(HttpPullPollUrlResolver.isAbsolute(null)).isFalse();
        assertThat(HttpPullPollUrlResolver.isAbsolute("  ")).isFalse();
    }
}
