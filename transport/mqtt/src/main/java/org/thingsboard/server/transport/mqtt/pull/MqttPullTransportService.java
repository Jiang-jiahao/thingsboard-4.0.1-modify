/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.ListeningExecutor;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.mqtt.MqttClient;
import org.thingsboard.mqtt.MqttClientConfig;
import org.thingsboard.mqtt.MqttConnectResult;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.profile.MqttPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullAuthConfiguration;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullAuthType;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullSubscribeRequest;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.pull.session.MqttPullCollectorSessionContext;
import org.thingsboard.server.transport.mqtt.pull.session.MqttPullTargetSession;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class MqttPullTransportService {

    private final TransportService transportService;
    private ListeningExecutor handlerExecutor;
    private ListeningExecutorService handlerExecutorService;

    @PostConstruct
    public void init() {
        handlerExecutorService = MoreExecutors.listeningDecorator(
                ThingsBoardExecutors.newWorkStealingPool(4, "mqtt-pull-handler"));
        handlerExecutor = new ListeningExecutor() {
            @Override
            public void execute(Runnable command) {
                handlerExecutorService.execute(command);
            }

            @Override
            public <T> com.google.common.util.concurrent.ListenableFuture<T> executeAsync(Callable<T> task) {
                return handlerExecutorService.submit(task);
            }
        };
    }

    @PreDestroy
    public void destroy() {
        if (handlerExecutorService != null) {
            handlerExecutorService.shutdownNow();
        }
    }

    public void connectAndSubscribe(MqttPullCollectorSessionContext sessionContext) {
        MqttPullDeviceProfileTransportConfiguration profile = sessionContext.getProfileTransportConfiguration();
        String brokerUrl = resolveBrokerUrl(sessionContext);
        MqttPullBrokerUrlResolver.BrokerEndpoint endpoint;
        try {
            endpoint = MqttPullBrokerUrlResolver.parse(brokerUrl);
        } catch (Exception e) {
            log.warn("[{}] Invalid MQTT broker URL: {}", sessionContext.getDeviceId(), brokerUrl, e);
            sessionContext.getTransportContext().scheduleReconnect(sessionContext);
            return;
        }
        MqttClientConfig config = new MqttClientConfig();
        config.setCleanSession(profile.getCleanSession() == null || profile.getCleanSession());
        int keepAlive = profile.getKeepAliveSec() != null ? profile.getKeepAliveSec() : 60;
        config.setTimeoutSeconds(keepAlive);
        applyAuth(config, profile.getAuth());
        String clientId = buildClientId(profile, sessionContext);
        if (StringUtils.isNotBlank(clientId)) {
            config.setClientId(clientId);
        }
        MqttClient client = MqttClient.create(config, (topic, payload) -> {
            onMessage(sessionContext, null, topic, payload);
            return Futures.immediateVoidFuture();
        }, handlerExecutor);
        sessionContext.setMqttClient(client);
        int connectTimeoutSec = Math.max(1, (profile.getConnectTimeoutMs() != null ? profile.getConnectTimeoutMs() : 10000) / 1000);
        try {
            MqttConnectResult result = client.connect(endpoint.getHost(), endpoint.getPort())
                    .get(connectTimeoutSec, TimeUnit.SECONDS);
            if (!result.isSuccess()) {
                throw new IllegalStateException("MQTT connect failed: " + result.getReturnCode());
            }
            subscribeAll(sessionContext, client);
            log.info("[{}] MQTT pull connected to {}:{}", sessionContext.getDeviceId(), endpoint.getHost(), endpoint.getPort());
            if (sessionContext.getTransportContext() != null) {
                sessionContext.getTransportContext().onMqttBrokerConnected(sessionContext);
            }
        } catch (Exception e) {
            log.warn("[{}] MQTT pull connect failed to {}", sessionContext.getDeviceId(), brokerUrl, e);
            disconnectQuietly(sessionContext);
            sessionContext.getTransportContext().scheduleReconnect(sessionContext);
        }
    }

    public void disconnectQuietly(MqttPullCollectorSessionContext sessionContext) {
        if (sessionContext.getTransportContext() != null) {
            sessionContext.getTransportContext().onMqttBrokerDisconnected(sessionContext);
        }
        MqttClient client = sessionContext.getMqttClient();
        sessionContext.setMqttClient(null);
        if (client != null) {
            try {
                client.disconnect();
            } catch (Exception ignored) {
            }
        }
    }

    private void subscribeAll(MqttPullCollectorSessionContext sessionContext, MqttClient client) {
        MqttPullDeviceProfileTransportConfiguration profile = sessionContext.getProfileTransportConfiguration();
        for (MqttPullSubscribeRequest request : profile.effectiveSubscribeRequests()) {
            MqttQoS qos = toQos(request.getQos());
            client.on(request.getTopic(), (topic, payload) -> {
                onMessage(sessionContext, request, topic, payload);
                return Futures.immediateVoidFuture();
            }, qos);
        }
    }

    private void onMessage(MqttPullCollectorSessionContext sessionContext, String topic, ByteBuf payload) {
        MqttPullDeviceProfileTransportConfiguration profile = sessionContext.getProfileTransportConfiguration();
        for (MqttPullSubscribeRequest request : profile.effectiveSubscribeRequests()) {
            if (topicMatches(request.getTopic(), topic)) {
                onMessage(sessionContext, request, topic, payload);
                return;
            }
        }
        onMessage(sessionContext, null, topic, payload);
    }

    private void onMessage(MqttPullCollectorSessionContext sessionContext, MqttPullSubscribeRequest request,
                           String topic, ByteBuf payload) {
        String body = payload.toString(StandardCharsets.UTF_8);
        if (request == null) {
            return;
        }
        HttpPullDeviceRoutingConfiguration routing = sessionContext.getProfileTransportConfiguration().resolveRouting(request);
        HttpPullPollDataType dataType = request.getDataType() != null ? request.getDataType() : HttpPullPollDataType.TELEMETRY;
        if (dataType == HttpPullPollDataType.TELEMETRY) {
            dispatchTelemetry(sessionContext, body, routing, topic);
        } else {
            dispatchAttributes(sessionContext, body, dataType == HttpPullPollDataType.SHARED_ATTRIBUTES, routing, topic);
        }
        if (MqttPullRoutingHelper.shouldRouteToMultipleDevices(routing, body)) {
            touchCollectorActivity(sessionContext);
        }
    }

    private void dispatchTelemetry(MqttPullCollectorSessionContext sessionContext, String body,
                                   HttpPullDeviceRoutingConfiguration routing, String topic) {
        String telemetryKey = routing != null ? routing.getTelemetryPayloadKey() : "mqttPullPayload";
        if (!MqttPullRoutingHelper.shouldRouteToMultipleDevices(routing, body)) {
            postTelemetry(sessionContext, sessionContext.getSessionInfo(), body, telemetryKey);
            return;
        }
        if (MqttPullRoutingHelper.isPerMessageMode(routing)) {
            routePerMessageTelemetry(sessionContext, body, routing, topic, telemetryKey);
            return;
        }
        List<Object> elements = MqttPullJsonHelper.readArrayElements(body, routing.getResponseArrayJsonPath());
        for (Object element : elements) {
            String externalId = MqttPullJsonHelper.readDeviceId(element, routing.getDeviceIdJsonPath());
            routeToTargetTelemetry(sessionContext, body, routing, telemetryKey, externalId,
                    MqttPullJsonHelper.elementToJsonString(element));
        }
    }

    private void dispatchAttributes(MqttPullCollectorSessionContext sessionContext, String body, boolean shared,
                                    HttpPullDeviceRoutingConfiguration routing, String topic) {
        if (!MqttPullRoutingHelper.shouldRouteToMultipleDevices(routing, body)) {
            postAttributes(sessionContext, sessionContext.getSessionInfo(), body, shared);
            return;
        }
        if (MqttPullRoutingHelper.isPerMessageMode(routing)) {
            routePerMessageAttributes(sessionContext, body, routing, topic, shared);
            return;
        }
        List<Object> elements = MqttPullJsonHelper.readArrayElements(body, routing.getResponseArrayJsonPath());
        for (Object element : elements) {
            String externalId = MqttPullJsonHelper.readDeviceId(element, routing.getDeviceIdJsonPath());
            MqttPullTargetSession target = findTargetSession(sessionContext, externalId);
            if (target == null || target.getSessionInfo() == null) {
                continue;
            }
            postAttributes(sessionContext, target.getSessionInfo(), MqttPullJsonHelper.elementToJsonString(element), shared);
        }
    }

    private void routePerMessageTelemetry(MqttPullCollectorSessionContext sessionContext, String body,
                                          HttpPullDeviceRoutingConfiguration routing, String topic, String telemetryKey) {
        String externalId = MqttPullDeviceIdResolver.resolve(topic, body, routing);
        routeToTargetTelemetry(sessionContext, body, routing, telemetryKey, externalId, body);
    }

    private void routePerMessageAttributes(MqttPullCollectorSessionContext sessionContext, String body,
                                         HttpPullDeviceRoutingConfiguration routing, String topic, boolean shared) {
        String externalId = MqttPullDeviceIdResolver.resolve(topic, body, routing);
        MqttPullTargetSession target = findTargetSession(sessionContext, externalId);
        if (target == null || target.getSessionInfo() == null) {
            log.debug("[{}] No MQTT pull target for per-message device id [{}]", sessionContext.getDeviceId(), externalId);
            return;
        }
        postAttributes(sessionContext, target.getSessionInfo(), body, shared);
    }

    private void routeToTargetTelemetry(MqttPullCollectorSessionContext sessionContext, String body,
                                        HttpPullDeviceRoutingConfiguration routing, String telemetryKey,
                                        String externalId, String payloadJson) {
        if (StringUtils.isBlank(externalId)) {
            return;
        }
        MqttPullTargetSession target = findTargetSession(sessionContext, externalId);
        if (target == null || target.getSessionInfo() == null) {
            log.debug("[{}] No MQTT pull target for device id [{}] (strategy={})",
                    sessionContext.getDeviceId(), externalId.trim(),
                    routing != null ? routing.getDeviceIdMatchStrategy() : null);
            return;
        }
        postTelemetry(sessionContext, target.getSessionInfo(), payloadJson, telemetryKey);
    }

    private MqttPullTargetSession findTargetSession(MqttPullCollectorSessionContext sessionContext, String externalId) {
        if (StringUtils.isBlank(externalId)) {
            return null;
        }
        return sessionContext.getActiveTargets().get(externalId.trim());
    }

    private void postTelemetry(MqttPullCollectorSessionContext collectorCtx,
                               TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, String telemetryKey) {
        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().activateMqttPullDeviceSession(sessionInfo, collectorCtx.getDeviceId());
        }
        String key = StringUtils.isNotBlank(telemetryKey) ? telemetryKey : "mqttPullPayload";
        JsonObject wrapper = new JsonObject();
        try {
            wrapper.add(key, JsonParser.parseString(jsonPayload));
        } catch (Exception e) {
            wrapper.addProperty(key, jsonPayload);
        }
        TransportProtos.PostTelemetryMsg msg = JsonConverter.convertToTelemetryProto(wrapper);
        transportService.process(sessionInfo, msg, null);
    }

    private void postAttributes(MqttPullCollectorSessionContext collectorCtx,
                                TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, boolean shared) {
        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().activateMqttPullDeviceSession(sessionInfo, collectorCtx.getDeviceId());
        }
        JsonElement parsed = JsonParser.parseString(jsonPayload);
        if (!parsed.isJsonObject()) {
            log.warn("MQTT pull attributes payload is not a JSON object, skipping");
            return;
        }
        TransportProtos.PostAttributeMsg.Builder builder = JsonConverter.convertToAttributesProto(parsed).toBuilder();
        builder.setShared(shared);
        transportService.process(sessionInfo, builder.build(), null);
    }

    /**
     * 多设备/按消息路由时，目标设备收到数据后同步刷新采集器活跃状态（不重复写入业务遥测）。
     */
    private void touchCollectorActivity(MqttPullCollectorSessionContext collectorCtx) {
        if (collectorCtx.getSessionInfo() == null) {
            return;
        }
        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().activateMqttPullDeviceSession(
                    collectorCtx.getSessionInfo(), collectorCtx.getDeviceId());
        }
        JsonObject wrapper = new JsonObject();
        wrapper.addProperty("mqttPullLastActivityTs", System.currentTimeMillis());
        TransportProtos.PostTelemetryMsg msg = JsonConverter.convertToTelemetryProto(wrapper);
        transportService.process(collectorCtx.getSessionInfo(), msg, null);
    }

    private String resolveBrokerUrl(MqttPullCollectorSessionContext ctx) {
        String override = ctx.getDeviceTransportConfiguration() != null
                ? ctx.getDeviceTransportConfiguration().getBrokerUrlOverride() : null;
        return MqttPullBrokerUrlResolver.resolve(ctx.getProfileTransportConfiguration().getBrokerUrl(), override);
    }

    private void applyAuth(MqttClientConfig config, MqttPullAuthConfiguration auth) {
        if (auth == null || auth.getAuthType() == null || auth.getAuthType() == MqttPullAuthType.NONE) {
            return;
        }
        if (auth.getAuthType() == MqttPullAuthType.USERNAME_PASSWORD) {
            config.setUsername(auth.getUsername());
            config.setPassword(auth.getPassword());
        }
    }

    private String buildClientId(MqttPullDeviceProfileTransportConfiguration profile, MqttPullCollectorSessionContext ctx) {
        String prefix = profile.getClientIdPrefix();
        if (StringUtils.isBlank(prefix)) {
            prefix = "tb-mqtt-pull";
        }
        String suffix = ctx.getDeviceId().getId().toString().replace("-", "").substring(0, 8);
        String clientId = prefix + "-" + suffix;
        return clientId.length() > 23 ? clientId.substring(0, 23) : clientId;
    }

    private MqttQoS toQos(Integer qos) {
        if (qos == null) {
            return MqttQoS.AT_LEAST_ONCE;
        }
        return switch (qos) {
            case 0 -> MqttQoS.AT_MOST_ONCE;
            case 2 -> MqttQoS.EXACTLY_ONCE;
            default -> MqttQoS.AT_LEAST_ONCE;
        };
    }

    private boolean topicMatches(String filter, String topic) {
        if (filter == null || topic == null) {
            return false;
        }
        if (filter.equals(topic)) {
            return true;
        }
        if (filter.endsWith("/#")) {
            String prefix = filter.substring(0, filter.length() - 2);
            return topic.startsWith(prefix);
        }
        if (filter.contains("+")) {
            String[] filterParts = filter.split("/");
            String[] topicParts = topic.split("/");
            if (filterParts.length != topicParts.length) {
                return false;
            }
            for (int i = 0; i < filterParts.length; i++) {
                if (!"+".equals(filterParts[i]) && !filterParts[i].equals(topicParts[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static String buildMatchKey(org.thingsboard.server.common.data.transport.http.HttpPullDeviceIdMatchStrategy strategy,
                                       TransportProtos.HttpPullRoutingTargetProto target) {
        return switch (strategy) {
            case DEVICE_LABEL -> target.getLabel();
            case EXTERNAL_DEVICE_ID -> target.getExternalDeviceId();
            default -> target.getName();
        };
    }
}
