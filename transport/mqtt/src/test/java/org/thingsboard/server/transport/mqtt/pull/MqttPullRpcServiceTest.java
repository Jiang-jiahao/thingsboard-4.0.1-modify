/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.thingsboard.mqtt.MqttClient;
import org.thingsboard.mqtt.MqttHandler;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.device.profile.DeviceProfileData;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.pull.session.MqttPullCollectorSessionContext;
import org.thingsboard.server.transport.mqtt.pull.session.PendingMqttPullRpc;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MqttPullRpcServiceTest {

    @Mock
    private TransportService transportService;
    @Mock
    private TransportDeviceProfileCache deviceProfileCache;
    @Mock
    private MqttClient mqttClient;

    private MqttPullRpcService rpcService;
    private MqttPullCollectorSessionContext ctx;
    private TransportProtos.SessionInfoProto sessionInfo;

    @BeforeEach
    public void setUp() {
        rpcService = new MqttPullRpcService(transportService, deviceProfileCache);
        DeviceProfile profile = profileWithCustomRpc();
        DeviceProfileId profileId = new DeviceProfileId(UUID.randomUUID());
        profile.setId(profileId);
        Device device = new Device();
        device.setId(new DeviceId(UUID.randomUUID()));
        device.setDeviceProfileId(profileId);
        device.setName("mqtt-pull-device");
        sessionInfo = TransportProtos.SessionInfoProto.newBuilder()
                .setSessionIdMSB(UUID.randomUUID().getMostSignificantBits())
                .setSessionIdLSB(UUID.randomUUID().getLeastSignificantBits())
                .build();
        ctx = MqttPullCollectorSessionContext.builder()
                .device(device)
                .deviceProfile(profile)
                .sessionInfo(sessionInfo)
                .build();
        lenient().when(deviceProfileCache.get(profileId)).thenReturn(profile);
    }

    @Test
    public void disconnectedClientSendsErrorResponseForOneWayRpc() {
        TransportProtos.ToDeviceRpcRequestMsg request = TransportProtos.ToDeviceRpcRequestMsg.newBuilder()
                .setRequestId(11)
                .setMethodName("jammer")
                .setParams("{\"prefix\":\"server/chan\"}")
                .setOneway(true)
                .setExpirationTime(System.currentTimeMillis() + 10_000)
                .build();

        rpcService.onToDeviceRpcRequest(ctx, request);

        ArgumentCaptor<TransportProtos.ToDeviceRpcResponseMsg> captor =
                ArgumentCaptor.forClass(TransportProtos.ToDeviceRpcResponseMsg.class);
        verify(transportService).process(eq(sessionInfo), captor.capture(), any(TransportServiceCallback.class));
        assertThat(captor.getValue().getPayload()).contains("MQTT pull client is not connected");
        verify(transportService, never()).process(eq(sessionInfo), eq(request), any(), any());
    }

    @Test
    public void resetRpcStateAllowsResubscribeAfterReconnect() throws Exception {
        stubConnectedClient();
        ctx.setMqttClient(mqttClient);
        ctx.getRpcResponseSubscriptions().add("server/chan/api/jammerresult");

        TransportProtos.ToDeviceRpcRequestMsg request = TransportProtos.ToDeviceRpcRequestMsg.newBuilder()
                .setRequestId(12)
                .setMethodName("jammer")
                .setParams("{\"prefix\":\"server/chan\"}")
                .setOneway(false)
                .setExpirationTime(System.currentTimeMillis() + 10_000)
                .build();

        rpcService.onToDeviceRpcRequest(ctx, request);
        verify(mqttClient, never()).on(anyString(), any(MqttHandler.class), any(MqttQoS.class));

        ctx.resetRpcState();
        rpcService.onToDeviceRpcRequest(ctx, request);
        verify(mqttClient).on(eq("server/chan/api/jammerresult"), any(MqttHandler.class), any(MqttQoS.class));
        verify(mqttClient, times(2)).publish(eq("server/chan/api/jammer"), any(ByteBuf.class), any(MqttQoS.class));
    }

    @Test
    public void failPendingRpcsSendsErrorAndClearsQueue() {
        TransportProtos.ToDeviceRpcRequestMsg request = TransportProtos.ToDeviceRpcRequestMsg.newBuilder()
                .setRequestId(13)
                .setMethodName("jammer")
                .setOneway(false)
                .build();
        ctx.getPendingRpcByResponseTopic()
                .computeIfAbsent("server/chan/api/jammerresult", t -> new ConcurrentLinkedQueue<>())
                .add(PendingMqttPullRpc.builder()
                        .requestId(13)
                        .request(request)
                        .sessionInfo(sessionInfo)
                        .responseTopic("server/chan/api/jammerresult")
                        .build());

        rpcService.failPendingRpcs(ctx, "MQTT pull client is not connected");

        assertThat(ctx.getPendingRpcByResponseTopic()).isEmpty();
        ArgumentCaptor<TransportProtos.ToDeviceRpcResponseMsg> captor =
                ArgumentCaptor.forClass(TransportProtos.ToDeviceRpcResponseMsg.class);
        verify(transportService).process(eq(sessionInfo), captor.capture(), any(TransportServiceCallback.class));
        assertThat(captor.getValue().getPayload()).contains("MQTT pull client is not connected");
    }

    @SuppressWarnings("unchecked")
    private void stubConnectedClient() throws Exception {
        Future<Void> ok = mock(Future.class);
        when(ok.get(anyLong(), any(TimeUnit.class))).thenReturn(null);
        when(mqttClient.isConnected()).thenReturn(true);
        when(mqttClient.publish(anyString(), any(ByteBuf.class), any(MqttQoS.class))).thenReturn(ok);
        when(mqttClient.on(anyString(), any(MqttHandler.class), any(MqttQoS.class))).thenReturn(ok);
    }

    private static DeviceProfile profileWithCustomRpc() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("jammer");
        method.setBindingType(DeviceProfileRpcBindingType.MQTT_CUSTOM);
        method.setMqttRequestTopic("${params.prefix}/api/jammer");
        method.setMqttResponseTopic("${params.prefix}/api/jammerresult");
        method.setMqttPayloadTemplate("${params}");
        DeviceProfileData data = new DeviceProfileData();
        data.setRpcMethods(List.of(method));
        DeviceProfile profile = new DeviceProfile();
        profile.setProfileData(data);
        return profile;
    }
}
