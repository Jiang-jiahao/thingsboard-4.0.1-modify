/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.scheduler.SchedulerComponent;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.after;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class MqttTransportContextTest {

    @Mock
    private TransportService transportService;

    private ScheduledExecutorService executor;
    private MqttTransportContext context;
    private TenantId tenantId;
    private DeviceId deviceId;

    @BeforeEach
    public void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        context = new MqttTransportContext();
        context.setTransportService(transportService);
        context.setScheduler(new ExecutorScheduler(executor));
        tenantId = TenantId.fromUUID(UUID.randomUUID());
        deviceId = new DeviceId(UUID.randomUUID());
    }

    @AfterEach
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void reconnectCancelsDelayedInactivity() {
        setDelayMs(80);
        TransportProtos.SessionInfoProto session = sessionInfo(UUID.randomUUID());
        context.registerMqttServerSession(session);
        context.scheduleDisconnectInactivity(session);
        context.registerMqttServerSession(sessionInfo(UUID.randomUUID()));

        verify(transportService, after(150).never()).reportDeviceInactivity(tenantId, deviceId);
    }

    @Test
    public void disconnectReportsInactivityAfterDelay() {
        setDelayMs(80);
        TransportProtos.SessionInfoProto session = sessionInfo(UUID.randomUUID());
        context.registerMqttServerSession(session);
        context.scheduleDisconnectInactivity(session);

        verify(transportService, after(250).times(1)).reportDeviceInactivity(tenantId, deviceId);
    }

    @Test
    public void flushReportsConnectedSessionImmediately() {
        setDelayMs(30_000);
        context.registerMqttServerSession(sessionInfo(UUID.randomUUID()));

        context.flushMqttServerDisconnectInactivity();

        verify(transportService, times(1)).reportDeviceInactivity(tenantId, deviceId);
        verify(transportService).closeLocalSessionsAndReportInactivity();
        verify(transportService).flushToCore();
    }

    @Test
    public void flushReportsPendingDisconnectImmediately() {
        setDelayMs(30_000);
        TransportProtos.SessionInfoProto session = sessionInfo(UUID.randomUUID());
        context.registerMqttServerSession(session);
        context.scheduleDisconnectInactivity(session);

        context.flushMqttServerDisconnectInactivity();

        verify(transportService, times(1)).reportDeviceInactivity(tenantId, deviceId);
    }

    @Test
    public void otherLiveSessionSkipsInactivity() {
        setDelayMs(0);
        TransportProtos.SessionInfoProto first = sessionInfo(UUID.randomUUID());
        TransportProtos.SessionInfoProto second = sessionInfo(UUID.randomUUID());
        context.registerMqttServerSession(first);
        context.registerMqttServerSession(second);

        context.scheduleDisconnectInactivity(first);
        verify(transportService, never()).reportDeviceInactivity(tenantId, deviceId);

        context.scheduleDisconnectInactivity(second);
        verify(transportService, times(1)).reportDeviceInactivity(tenantId, deviceId);
    }

    @Test
    public void shuttingDownReportsDisconnectImmediately() {
        setDelayMs(30_000);
        context.flushMqttServerDisconnectInactivity();
        context.scheduleDisconnectInactivity(sessionInfo(UUID.randomUUID()));
        verify(transportService, times(1)).reportDeviceInactivity(tenantId, deviceId);
    }

    private void setDelayMs(long delayMs) {
        ReflectionTestUtils.setField(context, "disconnectInactivityDelayMs", delayMs);
    }

    private TransportProtos.SessionInfoProto sessionInfo(UUID sessionId) {
        return TransportProtos.SessionInfoProto.newBuilder()
                .setTenantIdMSB(tenantId.getId().getMostSignificantBits())
                .setTenantIdLSB(tenantId.getId().getLeastSignificantBits())
                .setDeviceIdMSB(deviceId.getId().getMostSignificantBits())
                .setDeviceIdLSB(deviceId.getId().getLeastSignificantBits())
                .setSessionIdMSB(sessionId.getMostSignificantBits())
                .setSessionIdLSB(sessionId.getLeastSignificantBits())
                .build();
    }

    private static final class ExecutorScheduler implements SchedulerComponent {
        private final ScheduledExecutorService executor;

        private ExecutorScheduler(ScheduledExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return executor.schedule(command, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return executor.schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }
    }
}
