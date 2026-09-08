/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.ssl.SslHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileData;
import org.thingsboard.server.common.data.device.profile.JsonTransportPayloadConfiguration;
import org.thingsboard.server.common.data.device.profile.MqttDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.mqtt.MqttUplinkTopicMapping;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.adaptors.JsonMqttAdaptor;
import org.thingsboard.server.transport.mqtt.rpc.PendingMqttServerRpc;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class MqttTransportHandlerTest {

    public static final int MSG_QUEUE_LIMIT = 10;
    public static final InetSocketAddress IP_ADDR = new InetSocketAddress("127.0.0.1", 9876);
    public static final int TIMEOUT = 30;

    @Mock
    MqttTransportContext context;
    @Mock
    SslHandler sslHandler;
    @Mock
    ChannelHandlerContext ctx;

    AtomicInteger packedId = new AtomicInteger();
    ExecutorService executor;
    MqttTransportHandler handler;

    @Spy
    TransportService transportService;

    @BeforeEach
    public void setUp() throws Exception {

        lenient().doReturn(MSG_QUEUE_LIMIT).when(context).getMessageQueueSizePerDeviceLimit();
        lenient().doReturn(transportService).when(context).getTransportService();

        handler = spy(new MqttTransportHandler(context, sslHandler));
        lenient().doReturn(IP_ADDR).when(handler).getAddress(any());
    }

    @AfterEach
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    MqttConnectMessage getMqttConnectMessage() {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, true, MqttQoS.AT_LEAST_ONCE, false, 123);
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader("device", packedId.incrementAndGet(), true, true, true, 1, true, false, 60);
        MqttConnectPayload payload = new MqttConnectPayload("clientId", "topic", "message".getBytes(StandardCharsets.UTF_8), "username", "password".getBytes(StandardCharsets.UTF_8));
        return new MqttConnectMessage(mqttFixedHeader, variableHeader, payload);
    }

    MqttPublishMessage getMqttPublishMessage() {
        return getMqttPublishMessage("v1/gateway/telemetry");
    }

    MqttPublishMessage getDeviceMqttPublishMessage() {
        return getMqttPublishMessage("v1/devices/me/telemetry");
    }

    MqttPublishMessage getMqttPublishMessage(String topicName) {
        return getMqttPublishMessage(topicName, "{\"testKey\":\"testValue\"}");
    }

    MqttPublishMessage getMqttPublishMessage(String topicName, String payloadJson) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, true, MqttQoS.AT_LEAST_ONCE, false, 123);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, packedId.incrementAndGet());
        ByteBuf payload = Unpooled.wrappedBuffer(payloadJson.getBytes(StandardCharsets.UTF_8));
        return new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);
    }

    @Test
    public void givenMqttConnectMessage_whenProcessMqttMsg_thenProcessConnect() {
        MqttConnectMessage msg = getMqttConnectMessage();
        willDoNothing().given(handler).processConnect(ctx, msg);

        handler.channelRead(ctx, msg);

        assertThat(handler.address, is(IP_ADDR));
        assertThat(handler.deviceSessionCtx.getChannel(), is(ctx));
        verify(handler, never()).doDisconnect();
        verify(handler, times(1)).processConnect(ctx, msg);
    }

    @Test
    public void givenQueueLimit_whenEnqueueRegularSessionMsgOverLimit_thenOK() {
        List<MqttPublishMessage> messages = Stream.generate(this::getMqttPublishMessage).limit(MSG_QUEUE_LIMIT).collect(Collectors.toList());
        messages.forEach(msg -> handler.enqueueRegularSessionMsg(ctx, msg));
        assertThat(handler.deviceSessionCtx.getMsgQueueSize(), is(MSG_QUEUE_LIMIT));
        assertThat(handler.deviceSessionCtx.getMsgQueueSnapshot(), contains(messages.toArray()));
    }

    @Test
    public void givenQueueLimit_whenEnqueueRegularSessionMsgOverLimit_thenCtxClose() {
        final int limit = MSG_QUEUE_LIMIT + 1;
        willDoNothing().given(handler).processMsgQueue(ctx);
        List<MqttPublishMessage> messages = Stream.generate(this::getMqttPublishMessage).limit(limit).collect(Collectors.toList());

        messages.forEach((msg) -> handler.enqueueRegularSessionMsg(ctx, msg));

        assertThat(handler.deviceSessionCtx.getMsgQueueSize(), is(MSG_QUEUE_LIMIT));
        verify(handler, times(limit)).enqueueRegularSessionMsg(any(), any());
        verify(handler, times(MSG_QUEUE_LIMIT)).processMsgQueue(any());
        verify(ctx, times(1)).close();
    }

    @Test
    public void givenMqttConnectMessageAndPublishImmediately_whenProcessMqttMsg_thenEnqueueRegularSessionMsg() {
        givenMqttConnectMessage_whenProcessMqttMsg_thenProcessConnect();

        List<MqttPublishMessage> messages = Stream.generate(this::getMqttPublishMessage).limit(MSG_QUEUE_LIMIT).collect(Collectors.toList());

        messages.forEach((msg) -> handler.channelRead(ctx, msg));

        assertThat(handler.address, is(IP_ADDR));
        assertThat(handler.deviceSessionCtx.getChannel(), is(ctx));
        assertThat(handler.deviceSessionCtx.isConnected(), is(false));
        assertThat(handler.deviceSessionCtx.getMsgQueueSize(), is(MSG_QUEUE_LIMIT));
        assertThat(handler.deviceSessionCtx.getMsgQueueSnapshot(), contains(messages.toArray()));
        verify(handler, never()).doDisconnect();
        verify(handler, times(1)).processConnect(any(), any());
        verify(handler, times(MSG_QUEUE_LIMIT)).enqueueRegularSessionMsg(any(), any());
        verify(handler, never()).processRegularSessionMsg(any(), any());
        messages.forEach((msg) -> verify(handler, times(1)).enqueueRegularSessionMsg(ctx, msg));
    }

    @Test
    public void givenMessageQueue_whenProcessMqttMsgConcurrently_thenEnqueueRegularSessionMsg() throws InterruptedException {
        //given
        assertThat(handler.deviceSessionCtx.isConnected(), is(false));
        assertThat(MSG_QUEUE_LIMIT, greaterThan(2));
        List<MqttPublishMessage> messages = Stream.generate(this::getMqttPublishMessage).limit(MSG_QUEUE_LIMIT).collect(Collectors.toList());
        messages.forEach((msg) -> handler.enqueueRegularSessionMsg(ctx, msg));
        willDoNothing().given(handler).processRegularSessionMsg(any(), any());
        executor = Executors.newCachedThreadPool(ThingsBoardThreadFactory.forName(getClass().getName()));

        CountDownLatch readyLatch = new CountDownLatch(MSG_QUEUE_LIMIT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(MSG_QUEUE_LIMIT);

        Stream.iterate(0, i -> i + 1).limit(MSG_QUEUE_LIMIT).forEach(x ->
                executor.submit(() -> {
                    try {
                        readyLatch.countDown();
                        assertThat(startLatch.await(TIMEOUT, TimeUnit.SECONDS), is(true));
                        handler.processMsgQueue(ctx);
                        finishLatch.countDown();
                    } catch (Exception e) {
                        log.error("Failed to run processMsgQueue", e);
                        fail("Failed to run processMsgQueue");
                    }
                }));

        //when
        assertThat(readyLatch.await(TIMEOUT, TimeUnit.SECONDS), is(true));
        handler.deviceSessionCtx.setConnected(true);
        startLatch.countDown();
        assertThat(finishLatch.await(TIMEOUT, TimeUnit.SECONDS), is(true));

        //then
        assertThat(handler.deviceSessionCtx.getMsgQueueSize(), is(0));
        assertThat(handler.deviceSessionCtx.getMsgQueueSnapshot(), empty());
        verify(handler, times(MSG_QUEUE_LIMIT)).processRegularSessionMsg(any(), any());
        messages.forEach((msg) -> verify(handler, times(1)).processRegularSessionMsg(ctx, msg));
    }

    @Test
    public void givenMqttMessage_whenDeviceProfileMqttTransport_thenTopicAddedToMetadata() {
        MqttPublishMessage message = getDeviceMqttPublishMessage();
        when(context.getJsonMqttAdaptor()).thenReturn(new JsonMqttAdaptor());
        handler.deviceSessionCtx.setConnected(true);
        DeviceProfile deviceProfile = new DeviceProfile();
        DeviceProfileData deviceProfileData = new DeviceProfileData();
        MqttDeviceProfileTransportConfiguration mqttDeviceProfileTransportConfiguration = new MqttDeviceProfileTransportConfiguration();
        mqttDeviceProfileTransportConfiguration.setTransportPayloadTypeConfiguration(new JsonTransportPayloadConfiguration());
        deviceProfileData.setTransportConfiguration(mqttDeviceProfileTransportConfiguration);
        deviceProfile.setProfileData(deviceProfileData);
        deviceProfile.setTransportType(DeviceTransportType.MQTT);
        handler.deviceSessionCtx.setDeviceProfile(deviceProfile);

        handler.processRegularSessionMsg(ctx, message);

        TbMsgMetaData expectedMd = new TbMsgMetaData();
        expectedMd.putValue(DataConstants.MQTT_TOPIC, message.variableHeader().topicName());

        verify(transportService, times(1)).process(any(), (TransportProtos.PostTelemetryMsg) any(), eq(expectedMd), any());
    }

    @Test
    public void givenUplinkMappedRpcResponse_whenPendingCustomRpc_thenCompleteRpcAndIngestTelemetry() {
        setupUavServerDeviceProfile();
        registerPendingCustomRpc("server/chan/api/jammerresult", 42);
        MqttPublishMessage message = getMqttPublishMessage("server/chan/api/jammerresult",
                "{\"device_id\":0,\"sector_id\":7,\"ok\":true}");

        handler.processRegularSessionMsg(ctx, message);

        ArgumentCaptor<TransportProtos.ToDeviceRpcResponseMsg> captor =
                ArgumentCaptor.forClass(TransportProtos.ToDeviceRpcResponseMsg.class);
        verify(transportService).process(any(), captor.capture(), any());
        assertThat(captor.getValue().getRequestId(), is(42));
        assertThat(captor.getValue().getPayload(), is("{\"device_id\":0,\"sector_id\":7,\"ok\":true}"));
        verify(transportService).process(any(), (TransportProtos.PostTelemetryMsg) any(), any(), any());
    }

    @Test
    public void givenUplinkMappedAttributesRpcResponse_whenPendingCustomRpc_thenCompleteRpcAndIngestAttributes() {
        setupUavServerDeviceProfile(HttpPullPollDataType.CLIENT_ATTRIBUTES, null);
        registerPendingCustomRpc("server/chan/api/jammer/response", 7);
        MqttPublishMessage message = getMqttPublishMessage("server/chan/api/jammer/response",
                "{\"status\":\"ok\"}");

        handler.processRegularSessionMsg(ctx, message);

        ArgumentCaptor<TransportProtos.ToDeviceRpcResponseMsg> captor =
                ArgumentCaptor.forClass(TransportProtos.ToDeviceRpcResponseMsg.class);
        verify(transportService).process(any(), captor.capture(), any());
        assertThat(captor.getValue().getRequestId(), is(7));
        verify(transportService).process(any(), (TransportProtos.PostAttributeMsg) any(), any(), any());
    }

    @Test
    public void givenConnectedSession_whenDoDisconnect_thenScheduleDelayedInactivityWithoutRecordingClosedAsActivity() {
        TransportProtos.SessionInfoProto sessionInfo = TransportProtos.SessionInfoProto.newBuilder()
                .setDeviceIdMSB(1L)
                .setDeviceIdLSB(2L)
                .setTenantIdMSB(3L)
                .setTenantIdLSB(4L)
                .setSessionIdMSB(5L)
                .setSessionIdLSB(6L)
                .build();
        handler.deviceSessionCtx.setConnected(true);
        handler.deviceSessionCtx.setSessionInfo(sessionInfo);

        handler.doDisconnect();

        verify(transportService).process(eq(sessionInfo), any(TransportProtos.SessionEventMsg.class), isNull());
        verify(transportService).deregisterSession(sessionInfo);
        verify(context).scheduleDisconnectInactivity(sessionInfo);
        verify(transportService, never()).reportDeviceInactivity(any(), any());
        assertThat(handler.deviceSessionCtx.isConnected(), is(false));
    }

    @Test
    public void givenUplinkMappedJammerResult_whenNoPendingRpc_thenIngestTelemetry() {
        setupUavServerDeviceProfile();
        MqttPublishMessage message = getMqttPublishMessage("server/chan/api/jammerresult",
                "{\"device_id\":0,\"ok\":true}");

        handler.processRegularSessionMsg(ctx, message);

        verify(transportService).process(any(), (TransportProtos.PostTelemetryMsg) any(), any(), any());
        verify(transportService, never()).process(any(), any(TransportProtos.ToDeviceRpcResponseMsg.class), any());
    }

    private void setupUavServerDeviceProfile() {
        setupUavServerDeviceProfile("+/+/api/jammerresult", HttpPullPollDataType.TELEMETRY, "jammerresult");
    }

    private void setupUavServerDeviceProfile(HttpPullPollDataType dataType, String telemetryPayloadKey) {
        String topic = dataType == HttpPullPollDataType.TELEMETRY ? "+/+/api/jammerresult" : "+/+/api/jammer/response";
        setupUavServerDeviceProfile(topic, dataType, telemetryPayloadKey);
    }

    private void setupUavServerDeviceProfile(String topic, HttpPullPollDataType dataType, String telemetryPayloadKey) {
        when(context.getJsonMqttAdaptor()).thenReturn(new JsonMqttAdaptor());
        handler.deviceSessionCtx.setConnected(true);
        handler.deviceSessionCtx.setSessionInfo(TransportProtos.SessionInfoProto.getDefaultInstance());
        DeviceProfile deviceProfile = new DeviceProfile();
        DeviceProfileData deviceProfileData = new DeviceProfileData();
        MqttDeviceProfileTransportConfiguration mqttConfig = new MqttDeviceProfileTransportConfiguration();
        mqttConfig.setTransportPayloadTypeConfiguration(new JsonTransportPayloadConfiguration());
        MqttUplinkTopicMapping mapping = new MqttUplinkTopicMapping();
        mapping.setName("overlap");
        mapping.setEnabled(true);
        mapping.setTopic(topic);
        mapping.setDataType(dataType);
        mapping.setTelemetryPayloadKey(telemetryPayloadKey);
        mqttConfig.setUplinkTopicMappings(List.of(mapping));
        deviceProfileData.setTransportConfiguration(mqttConfig);
        deviceProfile.setProfileData(deviceProfileData);
        deviceProfile.setTransportType(DeviceTransportType.MQTT);
        handler.deviceSessionCtx.setDeviceProfile(deviceProfile);
    }

    @SuppressWarnings("unchecked")
    private void registerPendingCustomRpc(String responseTopic, int requestId) {
        ConcurrentMap<String, ConcurrentLinkedQueue<PendingMqttServerRpc>> pending =
                (ConcurrentMap<String, ConcurrentLinkedQueue<PendingMqttServerRpc>>)
                        ReflectionTestUtils.getField(handler, "pendingCustomRpcByResponseTopic");
        pending.computeIfAbsent(responseTopic, t -> new ConcurrentLinkedQueue<>())
                .add(PendingMqttServerRpc.builder()
                        .requestId(requestId)
                        .request(TransportProtos.ToDeviceRpcRequestMsg.newBuilder()
                                .setRequestId(requestId)
                                .setMethodName("jammer")
                                .build())
                        .responseTopic(responseTopic)
                        .build());
    }

}