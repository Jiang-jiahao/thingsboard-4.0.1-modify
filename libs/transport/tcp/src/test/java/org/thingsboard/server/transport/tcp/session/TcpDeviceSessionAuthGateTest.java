/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.tcp.session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.thingsboard.server.transport.tcp.TcpTransportContext;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 鉴权在途门控：设备在鉴权完成前重传首帧时，只丢弃该帧（早期实现会静默关闭连接），
 * 且上次鉴权响应丢失时必须有超时兜底，否则会话永久卡在"鉴权在途"。
 */
@ExtendWith(MockitoExtension.class)
class TcpDeviceSessionAuthGateTest {

    @Mock
    private TcpTransportContext tcpTransportContext;

    private TcpDeviceSession session;

    @BeforeEach
    void setUp() {
        session = new TcpDeviceSession(UUID.randomUUID(), tcpTransportContext, false);
    }

    @Test
    void frameWhileAuthInFlightIsRejectedAndLoggedOnce() {
        assertThat(session.tryBeginServerAuth()).isTrue();

        assertThat(session.tryBeginServerAuth()).isFalse();
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
