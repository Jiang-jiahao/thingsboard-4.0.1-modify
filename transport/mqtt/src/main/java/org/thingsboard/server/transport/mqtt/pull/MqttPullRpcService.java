/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import com.google.common.util.concurrent.Futures;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.mqtt.MqttClient;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.rpc.RpcStatus;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.mqtt.pull.session.MqttPullCollectorSessionContext;
import org.thingsboard.server.transport.mqtt.pull.session.PendingMqttPullRpc;
import org.thingsboard.server.transport.mqtt.rpc.MqttRpcCatalog;
import org.thingsboard.server.transport.mqtt.rpc.MqttRpcCommandFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class MqttPullRpcService {

    private static final String RPC_TIMEOUT_MESSAGE = "MQTT pull RPC timed out";

    private final TransportService transportService;
    private final TransportDeviceProfileCache deviceProfileCache;

    public void onToDeviceRpcRequest(MqttPullCollectorSessionContext collectorCtx,
                                     TransportProtos.ToDeviceRpcRequestMsg request) {
        if (collectorCtx == null || collectorCtx.isDestroyed()) {
            return;
        }
        TransportProtos.SessionInfoProto sessionInfo = collectorCtx.getSessionInfo();
        if (sessionInfo == null) {
            log.warn("[{}] MQTT pull RPC skipped: session is not ready", collectorCtx.getDeviceId());
            return;
        }
        var executor = collectorCtx.getTransportContext() != null
                ? collectorCtx.getTransportContext().getExecutor() : null;
        Runnable task = () -> {
            try {
                if (isRpcExpired(request)) {
                    respondTimeout(sessionInfo, request);
                    return;
                }
                executeRpc(collectorCtx, sessionInfo, request);
            } catch (Exception e) {
                log.warn("[{}] MQTT pull RPC [{}] failed", collectorCtx.getDeviceId(), request.getMethodName(), e);
                respondError(sessionInfo, request, e.getMessage());
            }
        };
        if (executor != null) {
            executor.execute(task);
        } else {
            task.run();
        }
    }

    public void failPendingRpcs(MqttPullCollectorSessionContext collectorCtx, String error) {
        if (collectorCtx == null || collectorCtx.getPendingRpcByResponseTopic().isEmpty()) {
            return;
        }
        for (ConcurrentLinkedQueue<PendingMqttPullRpc> queue : collectorCtx.getPendingRpcByResponseTopic().values()) {
            PendingMqttPullRpc pending;
            while (queue != null && (pending = queue.poll()) != null) {
                respondError(pending.getSessionInfo(), pending.getRequest(), error);
            }
        }
        collectorCtx.getPendingRpcByResponseTopic().clear();
    }

    public boolean tryCompletePendingRpc(MqttPullCollectorSessionContext collectorCtx, String topic, String payload) {
        if (collectorCtx == null || StringUtils.isBlank(topic) || collectorCtx.getPendingRpcByResponseTopic().isEmpty()) {
            return false;
        }
        ConcurrentLinkedQueue<PendingMqttPullRpc> exact = collectorCtx.getPendingRpcByResponseTopic().get(topic);
        PendingMqttPullRpc pending = takePending(collectorCtx, topic, payload, exact);
        if (pending == null) {
            for (var entry : collectorCtx.getPendingRpcByResponseTopic().entrySet()) {
                if (MqttRpcCommandFactory.topicMatches(entry.getKey(), topic)) {
                    pending = takePending(collectorCtx, entry.getKey(), payload, entry.getValue());
                    if (pending != null) {
                        break;
                    }
                }
            }
        }
        if (pending == null) {
            return false;
        }
        completeSuccess(pending, payload);
        return true;
    }

    private void executeRpc(MqttPullCollectorSessionContext collectorCtx,
                            TransportProtos.SessionInfoProto sessionInfo,
                            TransportProtos.ToDeviceRpcRequestMsg request) {
        DeviceProfileRpcMethod method = MqttRpcCatalog.find(resolveProfile(collectorCtx), request.getMethodName());
        MqttRpcCommandFactory.Command command = finalizeTopics(MqttRpcCommandFactory.resolve(
                method, request, collectorCtx.getDevice(), null, true));
        if (command.isUseStandardNativeTopic() || StringUtils.isBlank(command.getRequestTopic())) {
            throw new IllegalArgumentException("MQTT pull RPC requires mqttRequestTopic");
        }
        MqttClient client = collectorCtx.getMqttClient();
        if (client == null || !client.isConnected()) {
            throw new IllegalStateException("MQTT pull client is not connected");
        }
        if (!request.getOneway() && StringUtils.isBlank(command.getResponseTopic())) {
            throw new IllegalArgumentException("Two-way MQTT pull RPC requires mqttResponseTopic");
        }
        if (!request.getOneway() && StringUtils.isNotBlank(command.getResponseTopic())) {
            registerPending(collectorCtx, sessionInfo, command, request);
            subscribeResponseTopic(collectorCtx, client, command);
        }
        byte[] bytes = command.getPayload() != null
                ? command.getPayload().getBytes(StandardCharsets.UTF_8) : new byte[0];
        MqttQoS qos = toQos(command.getQos());
        log.info("[{}] MQTT pull RPC [{}] publish topic [{}] qos [{}]",
                collectorCtx.getDeviceId(), request.getMethodName(), command.getRequestTopic(), qos.value());
        try {
            client.publish(command.getRequestTopic(), Unpooled.copiedBuffer(bytes), qos)
                    .get(Math.max(1, remainingRpcSeconds(request)), TimeUnit.SECONDS);
        } catch (Exception e) {
            removePending(collectorCtx, command.getResponseTopic(), request.getRequestId());
            throw new IllegalStateException("MQTT pull RPC publish failed: " + e.getMessage(), e);
        }
        transportService.process(sessionInfo, request, RpcStatus.DELIVERED, TransportServiceCallback.EMPTY);
    }

    private void registerPending(MqttPullCollectorSessionContext collectorCtx,
                                 TransportProtos.SessionInfoProto sessionInfo,
                                 MqttRpcCommandFactory.Command command,
                                 TransportProtos.ToDeviceRpcRequestMsg request) {
        PendingMqttPullRpc pending = PendingMqttPullRpc.builder()
                .requestId(request.getRequestId())
                .request(request)
                .sessionInfo(sessionInfo)
                .responseTopic(command.getResponseTopic())
                .build();
        collectorCtx.getPendingRpcByResponseTopic()
                .computeIfAbsent(command.getResponseTopic(), t -> new ConcurrentLinkedQueue<>())
                .add(pending);
        long delayMs = remainingRpcMillis(request);
        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().getScheduler().schedule(() -> {
                if (removePending(collectorCtx, command.getResponseTopic(), request.getRequestId()) != null) {
                    respondTimeout(sessionInfo, request);
                }
            }, Math.max(1L, delayMs), TimeUnit.MILLISECONDS);
        }
    }

    private void subscribeResponseTopic(MqttPullCollectorSessionContext collectorCtx, MqttClient client,
                                        MqttRpcCommandFactory.Command command) {
        String topic = command.getResponseTopic();
        if (!collectorCtx.getRpcResponseSubscriptions().add(topic)) {
            return;
        }
        try {
            client.on(topic, (t, payload) -> {
                onRpcResponsePayload(collectorCtx, t, payload);
                return Futures.immediateVoidFuture();
            }, toQos(command.getQos())).get(15, TimeUnit.SECONDS);
        } catch (Exception e) {
            collectorCtx.getRpcResponseSubscriptions().remove(topic);
            throw new IllegalStateException("MQTT pull RPC subscribe failed for " + topic, e);
        }
    }

    private void onRpcResponsePayload(MqttPullCollectorSessionContext collectorCtx, String topic, ByteBuf payload) {
        String body = payload != null ? payload.toString(StandardCharsets.UTF_8) : "";
        if (!tryCompletePendingRpc(collectorCtx, topic, body)) {
            log.debug("[{}] MQTT pull RPC response on unmatched topic [{}]", collectorCtx.getDeviceId(), topic);
        }
    }

    private PendingMqttPullRpc takePending(MqttPullCollectorSessionContext collectorCtx, String mapKey,
                                           String payload, ConcurrentLinkedQueue<PendingMqttPullRpc> queue) {
        if (queue == null || queue.isEmpty()) {
            return null;
        }
        Integer extractedId = MqttRpcCommandFactory.tryExtractRequestId(payload);
        if (extractedId != null) {
            for (PendingMqttPullRpc p : queue) {
                if (p.getRequestId() == extractedId && queue.remove(p)) {
                    if (queue.isEmpty()) {
                        collectorCtx.getPendingRpcByResponseTopic().remove(mapKey, queue);
                    }
                    return p;
                }
            }
        }
        PendingMqttPullRpc p = queue.poll();
        if (queue.isEmpty()) {
            collectorCtx.getPendingRpcByResponseTopic().remove(mapKey, queue);
        }
        return p;
    }

    private PendingMqttPullRpc removePending(MqttPullCollectorSessionContext collectorCtx, String responseTopic, int requestId) {
        if (collectorCtx == null || StringUtils.isBlank(responseTopic)) {
            return null;
        }
        ConcurrentLinkedQueue<PendingMqttPullRpc> queue = collectorCtx.getPendingRpcByResponseTopic().get(responseTopic);
        if (queue == null) {
            return null;
        }
        for (PendingMqttPullRpc p : queue) {
            if (p.getRequestId() == requestId && queue.remove(p)) {
                if (queue.isEmpty()) {
                    collectorCtx.getPendingRpcByResponseTopic().remove(responseTopic, queue);
                }
                return p;
            }
        }
        return null;
    }

    private void completeSuccess(PendingMqttPullRpc pending, String payload) {
        String body = MqttRpcCommandFactory.normalizeResponsePayload(payload);
        transportService.process(pending.getSessionInfo(),
                TransportProtos.ToDeviceRpcResponseMsg.newBuilder()
                        .setRequestId(pending.getRequestId())
                        .setPayload(body)
                        .build(),
                TransportServiceCallback.EMPTY);
    }

    private MqttRpcCommandFactory.Command finalizeTopics(MqttRpcCommandFactory.Command command) {
        String requestTopic = stripLeadingSlashes(command.getRequestTopic());
        String responseTopic = stripLeadingSlashes(command.getResponseTopic());
        requireConcreteTopic(requestTopic, "request");
        requireConcreteTopic(responseTopic, "response");
        return command.toBuilder()
                .requestTopic(requestTopic)
                .responseTopic(responseTopic)
                .build();
    }

    /** 前缀参数为空时 ${params.prefix}/api/... 会变成 /api/...，去掉开头的 /。 */
    private static String stripLeadingSlashes(String topic) {
        if (StringUtils.isBlank(topic)) {
            return topic;
        }
        String t = topic.trim();
        while (t.startsWith("/")) {
            t = t.substring(1);
        }
        return t;
    }

    private static void requireConcreteTopic(String topic, String kind) {
        if (StringUtils.isBlank(topic)) {
            return;
        }
        if (topic.contains("+") || topic.contains("#") || topic.contains("//")) {
            throw new IllegalStateException(
                    "MQTT pull RPC " + kind + " topic is not concrete. Pass topic params: " + topic);
        }
    }

    private DeviceProfile resolveProfile(MqttPullCollectorSessionContext collectorCtx) {
        Device device = collectorCtx.getDevice();
        if (device != null && device.getDeviceProfileId() != null) {
            DeviceProfile cached = deviceProfileCache.get(device.getDeviceProfileId());
            if (cached != null) {
                return cached;
            }
        }
        return collectorCtx.getDeviceProfile();
    }

    private static MqttQoS toQos(int qos) {
        return switch (qos) {
            case 0 -> MqttQoS.AT_MOST_ONCE;
            case 2 -> MqttQoS.EXACTLY_ONCE;
            default -> MqttQoS.AT_LEAST_ONCE;
        };
    }

    private static boolean isRpcExpired(TransportProtos.ToDeviceRpcRequestMsg request) {
        return request.getExpirationTime() > 0 && remainingRpcMillis(request) <= 0;
    }

    private static long remainingRpcMillis(TransportProtos.ToDeviceRpcRequestMsg request) {
        if (request.getExpirationTime() <= 0) {
            return 10000L;
        }
        return request.getExpirationTime() - System.currentTimeMillis();
    }

    private static long remainingRpcSeconds(TransportProtos.ToDeviceRpcRequestMsg request) {
        return Math.max(1L, remainingRpcMillis(request) / 1000L);
    }

    private void respondTimeout(TransportProtos.SessionInfoProto sessionInfo,
                                TransportProtos.ToDeviceRpcRequestMsg request) {
        respondError(sessionInfo, request, RPC_TIMEOUT_MESSAGE);
    }

    private void respondError(TransportProtos.SessionInfoProto sessionInfo,
                              TransportProtos.ToDeviceRpcRequestMsg request, String error) {
        if (sessionInfo == null || request == null) {
            return;
        }
        var node = JacksonUtil.newObjectNode();
        node.put("error", error != null ? error : "MQTT pull RPC failed");
        respondJson(sessionInfo, request, JacksonUtil.toString(node), false);
    }

    private void respondJson(TransportProtos.SessionInfoProto sessionInfo,
                             TransportProtos.ToDeviceRpcRequestMsg request,
                             String payload, boolean delivered) {
        if (delivered) {
            transportService.process(sessionInfo, request, RpcStatus.DELIVERED, TransportServiceCallback.EMPTY);
        }
        transportService.process(sessionInfo,
                TransportProtos.ToDeviceRpcResponseMsg.newBuilder()
                        .setRequestId(request.getRequestId())
                        .setPayload(payload)
                        .build(),
                TransportServiceCallback.EMPTY);
    }
}
