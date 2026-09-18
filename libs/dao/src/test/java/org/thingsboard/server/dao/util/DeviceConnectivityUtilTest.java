package org.thingsboard.server.dao.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceConnectivityUtilTest {

    @Test
    void testRootCaPemNaming() {
        assertThat(DeviceConnectivityUtil.CA_ROOT_CERT_PEM).contains("root");
        assertThat(DeviceConnectivityUtil.CA_ROOT_CERT_PEM).contains("ca");
        assertThat(DeviceConnectivityUtil.CA_ROOT_CERT_PEM).endsWith(".pem");
        assertThat(DeviceConnectivityUtil.CA_ROOT_CERT_PEM).doesNotContainAnyWhitespaces();
    }

}
