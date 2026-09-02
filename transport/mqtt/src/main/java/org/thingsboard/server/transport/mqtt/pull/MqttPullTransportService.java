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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.ListeningExecutor;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.mqtt.MqttClient;
import org.thingsboard.mqtt.MqttClientCallback;
import org.thingsboard.mqtt.MqttClientConfig;
import org.thingsboard.mqtt.MqttConnectResult;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.profile.MqttPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullAuthConfiguration;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullAuthType;
import org.thingsboard.server.common.data.transport.mqtt.MqttPullSubscribeRequest;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.pull.session.MqttPullCollectorSessionContext;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class MqttPullTransportService {

    private final TransportService transportService;
    private ListeningExecutor handlerExecutor;
    private ListeningExecutorService handlerExecutorService;

    @Value("${transport.mqtt.netty.max_payload_size:65536}")
    private int maxPayloadSize;

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
        config.setReconnect(false);
        config.setMaxBytesInMessage(Math.max(8092, maxPayloadSize));
        int keepAlive = profile.getKeepAliveSec() != null ? profile.getKeepAliveSec() : 60;
        config.setTimeoutSeconds(keepAlive);
        applyAuth(config, sessionContext.getDeviceTransportConfiguration() != null
                ? sessionContext.getDeviceTransportConfiguration().getAuth() : null);
        String clientId = buildClientId(sessionContext);
        if (StringUtils.isNotBlank(clientId)) {
            config.setClientId(clientId);
        }
        MqttClient client = MqttClient.create(config, (topic, payload) -> {
            dispatchByTopic(sessionContext, topic, payload);
            return Futures.immediateVoidFuture();
        }, handlerExecutor);
        client.setCallback(new MqttClientCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                if (sessionContext.getMqttClient() != client) {
                    return;
                }
                log.warn("[{}] MQTT pull connection lost", sessionContext.getDeviceId(), cause);
                if (sessionContext.getTransportContext() != null) {
                    sessionContext.getTransportContext().scheduleReconnect(sessionContext);
                }
            }

            @Override
            public void onSuccessfulReconnect() {
            }
        });
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
            try {
                client.on(request.getTopic(), (topic, payload) -> {
                    onMessage(sessionContext, request, topic, payload);
                    return Futures.immediateVoidFuture();
                }, qos).get(15, TimeUnit.SECONDS);
                log.info("[{}] MQTT pull subscribed topic [{}] qos [{}] dataType [{}]",
                        sessionContext.getDeviceId(), request.getTopic(), qos.value(), request.getDataType());
            } catch (Exception e) {
                throw new IllegalStateException("MQTT subscribe failed for topic " + request.getTopic(), e);
            }
        }
    }

    private void dispatchByTopic(MqttPullCollectorSessionContext sessionContext, String topic, ByteBuf payload) {
        MqttPullDeviceProfileTransportConfiguration profile = sessionContext.getProfileTransportConfiguration();
        boolean matched = false;
        for (MqttPullSubscribeRequest request : profile.effectiveSubscribeRequests()) {
            if (topicMatches(request.getTopic(), topic)) {
                matched = true;
                onMessage(sessionContext, request, topic, payload);
            }
        }
        if (!matched) {
            log.debug("[{}] MQTT pull message on unmatched topic [{}]", sessionContext.getDeviceId(), topic);
        }
    }

    private void onMessage(MqttPullCollectorSessionContext sessionContext, MqttPullSubscribeRequest request,
                           String topic, ByteBuf payload) {
        try {
            String body = payload.toString(StandardCharsets.UTF_8);
            if (request == null) {
                log.debug("[{}] MQTT pull message on unmatched topic [{}]", sessionContext.getDeviceId(), topic);
                return;
            }
            log.info("[{}] MQTT pull received topic [{}] request [{}] bytes [{}]",
                    sessionContext.getDeviceId(), topic, request.getName(), body.length());
            HttpPullPollDataType dataType = request.getDataType() != null ? request.getDataType() : HttpPullPollDataType.TELEMETRY;
            if (dataType == HttpPullPollDataType.TELEMETRY) {
                postTelemetry(sessionContext, sessionContext.getSessionInfo(), body, request.resolveTelemetryPayloadKey());
            } else {
                postAttributes(sessionContext, sessionContext.getSessionInfo(), body,
                        dataType == HttpPullPollDataType.SHARED_ATTRIBUTES);
            }
        } catch (Exception e) {
            log.warn("[{}] Failed to process MQTT pull message on topic [{}] request [{}]",
                    sessionContext.getDeviceId(), topic, request != null ? request.getName() : null, e);
        }
    }

    private void postTelemetry(MqttPullCollectorSessionContext collectorCtx,
                               TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, String telemetryKey) {
        if (sessionInfo == null) {
            log.warn("[{}] Skip telemetry: MQTT pull session is not ready", collectorCtx.getDeviceId());
            return;
        }
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
        transportService.process(sessionInfo, msg, TransportServiceCallback.EMPTY);
    }

    private void postAttributes(MqttPullCollectorSessionContext collectorCtx,
                                TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, boolean shared) {
        if (sessionInfo == null) {
            log.warn("[{}] Skip attributes: MQTT pull session is not ready", collectorCtx.getDeviceId());
            return;
        }
        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().activateMqttPullDeviceSession(sessionInfo, collectorCtx.getDeviceId());
        }
        JsonElement parsed;
        try {
            parsed = JsonParser.parseString(jsonPayload);
        } catch (Exception e) {
            log.warn("[{}] MQTT pull attributes payload is not valid JSON, skipping", collectorCtx.getDeviceId());
            return;
        }
        if (!parsed.isJsonObject()) {
            log.warn("[{}] MQTT pull attributes payload is not a JSON object, skipping", collectorCtx.getDeviceId());
            return;
        }
        TransportProtos.PostAttributeMsg.Builder builder = JsonConverter.convertToAttributesProto(parsed).toBuilder();
        builder.setShared(shared);
        transportService.process(sessionInfo, builder.build(), TransportServiceCallback.EMPTY);
    }

    private String resolveBrokerUrl(MqttPullCollectorSessionContext ctx) {
        return ctx.getDeviceTransportConfiguration() != null
                ? ctx.getDeviceTransportConfiguration().getBrokerUrl() : null;
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

    private String buildClientId(MqttPullCollectorSessionContext ctx) {
        String deviceClientId = ctx.getDeviceTransportConfiguration() != null
                ? ctx.getDeviceTransportConfiguration().getClientId() : null;
        if (StringUtils.isNotBlank(deviceClientId)) {
            return deviceClientId.trim();
        }
        String suffix = ctx.getDeviceId().getId().toString().replace("-", "").substring(0, 8);
        String clientId = "tb-mqtt-pull-" + suffix;
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
}
