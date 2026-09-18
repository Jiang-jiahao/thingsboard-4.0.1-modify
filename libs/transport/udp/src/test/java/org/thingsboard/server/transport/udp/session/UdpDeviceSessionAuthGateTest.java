package org.thingsboard.server.transport.udp.session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.thingsboard.server.transport.udp.UdpTransportContext;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * UDP 侧与 TCP 同构的鉴权在途门控：额外数据报只丢弃不关会话，并有超时兜底。
 */
@ExtendWith(MockitoExtension.class)
class UdpDeviceSessionAuthGateTest {

    @Mock
    private UdpTransportContext udpTransportContext;

    private UdpDeviceSession session;

    @BeforeEach
    void setUp() {
        session = new UdpDeviceSession(UUID.randomUUID(), udpTransportContext, false);
    }

    @Test
    void datagramWhileAuthInFlightIsRejectedAndLoggedOnce() {
        assertThat(session.tryBeginServerAuth()).isTrue();

        assertThat(session.tryBeginServerAuth()).isFalse();
        assertThat(session.shouldLogPreAuthDrop()).isTrue();
        assertThat(session.shouldLogPreAuthDrop()).isFalse();
    }

    @Test
    void endServerAuthAllowsRetry() {
        assertThat(session.tryBeginServerAuth()).isTrue();
        session.endServerAuth();

        assertThat(session.tryBeginServerAuth()).isTrue();
    }

    @Test
    void authInFlightIsReleasedAfterTimeout() throws Exception {
        ReflectionTestUtils.setField(session, "serverAuthTimeoutMs", 50L);
        assertThat(session.tryBeginServerAuth()).isTrue();
        assertThat(session.tryBeginServerAuth()).isFalse();

        Thread.sleep(120);

        assertThat(session.tryBeginServerAuth()).isTrue();
    }
}
