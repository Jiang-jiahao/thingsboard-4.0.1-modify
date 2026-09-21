package com.jnks.iot.server.coapserver;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.common.transport.config.ssl.SslCredentialsConfig;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = JnksIotCoapDtlsSettings.class)
@TestPropertySource(properties = {
        "coap.dtls.enabled=true",
        "coap.dtls.bind_address=192.168.1.1",
        "coap.dtls.bind_port=1234",
        "coap.dtls.retransmission_timeout=100",
        "coap.dtls.connection_id_length=500",
        "coap.dtls.x509.skip_validity_check_for_client_cert=true",
        "coap.dtls.x509.dtls_session_inactivity_timeout=1000",
        "coap.dtls.x509.dtls_session_report_timeout=3000",
})
class JnksIotCoapDtlsSettingsTest {

    @Autowired
    JnksIotCoapDtlsSettings coapDtlsSettings;
    @MockBean
    SslCredentialsConfig sslCredentialsConfig;
    @MockBean
    private TransportService transportService;
    @MockBean
    private JnksIotServiceInfoProvider serviceInfoProvider;

    @Test
    public void testCoapDtlsProperties() {
        assertThat(coapDtlsSettings).as("bean created").isNotNull();
        assertThat(coapDtlsSettings.getHost()).as("host").isEqualTo("192.168.1.1");
        assertThat(coapDtlsSettings.getPort()).as("port").isEqualTo(1234);
        assertThat(coapDtlsSettings.getDtlsRetransmissionTimeout()).as("retransmission_timeout").isEqualTo(100);
        assertThat(coapDtlsSettings.isSkipValidityCheckForClientCert()).as("skip_validity_check_for_client_cert").isTrue();
        assertThat(coapDtlsSettings.getDtlsSessionInactivityTimeout()).as("dtls_session_inactivity_timeout").isEqualTo(1000);
        assertThat(coapDtlsSettings.getDtlsSessionReportTimeout()).as("dtls_session_report_timeout").isEqualTo(3000);
    }

}
