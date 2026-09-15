/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.rpc.RpcStatus;
import org.thingsboard.server.common.data.transport.http.HttpPullAuthConfiguration;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.http.pull.session.HttpPullCollectorSessionContext;
import org.thingsboard.server.transport.http.outbound.HttpOutboundSessionContext;

import java.util.List;
import java.util.concurrent.ExecutorService;

@Service
@RequiredArgsConstructor
@Slf4j
public class HttpPullRpcService {

    private static final String RPC_TIMEOUT_MESSAGE = "HTTP outbound RPC timed out";

    private final HttpOutboundRpcExecutor outboundRpcExecutor;
    private final TransportService transportService;
    private final TransportDeviceProfileCache deviceProfileCache;

    @PostConstruct
    public void init() {
        // executor owns HttpClient lifecycle
    }

    public void onToDeviceRpcRequest(HttpPullCollectorSessionContext collectorCtx,
                                     TransportProtos.ToDeviceRpcRequestMsg request) {
        TransportProtos.SessionInfoProto sessionInfo = collectorCtx.getSessionInfo();
        Device device = collectorCtx.getDevice();
        DeviceProfileRpcMethod rpcMethod = findRpcMethod(device, collectorCtx.getDeviceProfile(), request.getMethodName());
        if (rpcMethod == null) {
            log.warn("[{}] HTTP pull RPC method not found: {}", collectorCtx.getDeviceId(), request.getMethodName());
            respondError(sessionInfo, request, "RPC method not found: " + request.getMethodName());
            return;
        }
        if (rpcMethod.getBindingType() != DeviceProfileRpcBindingType.HTTP_OUTBOUND) {
            return;
        }
        ExecutorService executor = collectorCtx.getTransportContext() != null
                ? collectorCtx.getTransportContext().getExecutor() : null;
        Runnable task = () -> {
            try {
                if (isRpcExpired(request)) {
                    respondTimeout(sessionInfo, request);
                    return;
                }
                executeOutboundRpc(collectorCtx.getDeviceId(), device,
                        collectorCtx.getDeviceTransportConfiguration(),
                        collectorCtx.getProfileTransportConfiguration() != null
                                ? collectorCtx.getProfileTransportConfiguration().getAuth() : null,
                        sessionInfo, request, rpcMethod,
                        () -> {
                            if (collectorCtx.getTransportContext() != null) {
                                collectorCtx.getTransportContext().activateHttpPullDeviceSession(
                                        sessionInfo, collectorCtx.getDeviceId());
                            }
                        });
            } catch (Exception e) {
                log.warn("[{}] HTTP outbound RPC [{}] failed", collectorCtx.getDeviceId(),
                        request.getMethodName(), e);
                String message = e instanceof RpcDeadlineExceededException ? RPC_TIMEOUT_MESSAGE : e.getMessage();
                respondError(sessionInfo, request, message);
            }
        };
        if (executor != null) {
            executor.execute(task);
        } else {
            task.run();
        }
    }

    public void onToDeviceRpcRequest(HttpOutboundSessionContext outboundCtx,
                                     TransportProtos.ToDeviceRpcRequestMsg request) {
        TransportProtos.SessionInfoProto sessionInfo = outboundCtx.getSessionInfo();
        Device device = outboundCtx.getDevice();
        DeviceProfileRpcMethod rpcMethod = findRpcMethod(device, outboundCtx.getDeviceProfile(), request.getMethodName());
        if (rpcMethod == null) {
            respondError(sessionInfo, request, "RPC method not found: " + request.getMethodName());
            return;
        }
        if (rpcMethod.getBindingType() != DeviceProfileRpcBindingType.HTTP_OUTBOUND) {
            // NATIVE 由设备 long-poll 会话处理；此处静默忽略避免与 long-poll 双投冲突
            return;
        }
        ExecutorService executor = outboundCtx.getTransportContext() != null
                ? outboundCtx.getTransportContext().getExecutor() : null;
        Runnable task = () -> {
            try {
                if (isRpcExpired(request)) {
                    respondTimeout(sessionInfo, request);
                    return;
                }
                executeOutboundRpc(outboundCtx.getDeviceId(), device, null, null,
                        sessionInfo, request, rpcMethod, null);
            } catch (Exception e) {
                log.warn("[{}] HTTP outbound RPC [{}] failed", outboundCtx.getDeviceId(),
                        request.getMethodName(), e);
                String message = e instanceof RpcDeadlineExceededException ? RPC_TIMEOUT_MESSAGE : e.getMessage();
                respondError(sessionInfo, request, message);
            }
        };
        if (executor != null) {
            executor.execute(task);
        } else {
            task.run();
        }
    }

    public void executeScheduledOutboundRpc(HttpPullCollectorSessionContext collectorCtx,
                                            DeviceProfileRpcMethod rpcMethod) throws Exception {
        if (rpcMethod == null || !rpcMethod.isScheduleActive()) {
            return;
        }
        Device device = collectorCtx.getDevice();
        HttpPullDeviceTransportConfiguration deviceCfg = collectorCtx.getDeviceTransportConfiguration();
        String urlOverride = deviceCfg != null ? deviceCfg.getPollUrlOverride() : null;
        HttpPullAuthConfiguration auth = collectorCtx.getProfileTransportConfiguration() != null
                ? collectorCtx.getProfileTransportConfiguration().getAuth() : null;
        int readTimeoutMs = resolveScheduledReadTimeoutMs(rpcMethod);
        HttpOutboundRpcExecutor.OutboundHttpResult result = outboundRpcExecutor.execute(
                collectorCtx.getDeviceId(), device, deviceCfg, auth, rpcMethod, "{}", urlOverride, readTimeoutMs, 0);
        if (result.statusCode() < 200 || result.statusCode() >= 300) {
            log.warn("[{}] Scheduled HTTP outbound RPC [{}] manufacturer error HTTP {}: {}",
                    collectorCtx.getDeviceId(), rpcMethod.getId(), result.statusCode(), truncate(result.body()));
            return;
        }
        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().activateHttpPullDeviceSession(
                    collectorCtx.getSessionInfo(), collectorCtx.getDeviceId());
        }
        log.debug("[{}] Scheduled HTTP outbound RPC [{}] ok HTTP {}",
                collectorCtx.getDeviceId(), rpcMethod.getId(), result.statusCode());
    }

    private void executeOutboundRpc(DeviceId deviceId, Device targetDevice,
                                    HttpPullDeviceTransportConfiguration targetDeviceCfg,
                                    HttpPullAuthConfiguration auth,
                                    TransportProtos.SessionInfoProto sessionInfo,
                                    TransportProtos.ToDeviceRpcRequestMsg request,
                                    DeviceProfileRpcMethod rpcMethod,
                                    Runnable onDelivered) throws Exception {
        String paramsJson = request.getParams() != null ? request.getParams() : "{}";
        int readTimeoutMs = resolveRpcReadTimeoutMs(request);
        String urlOverride = targetDeviceCfg != null ? targetDeviceCfg.getPollUrlOverride() : null;
        HttpOutboundRpcExecutor.OutboundHttpResult result = outboundRpcExecutor.execute(
                deviceId, targetDevice, targetDeviceCfg, auth, rpcMethod, paramsJson, urlOverride, readTimeoutMs,
                request.getRequestId());

        if (result.statusCode() < 200 || result.statusCode() >= 300) {
            respondManufacturerError(sessionInfo, request, result.statusCode(), result.body());
            return;
        }

        if (onDelivered != null) {
            onDelivered.run();
        }
        transportService.process(sessionInfo, request, RpcStatus.DELIVERED, TransportServiceCallback.EMPTY);
        if (!request.getOneway()) {
            String payload = normalizeRpcResponsePayload(result.body());
            transportService.process(sessionInfo,
                    TransportProtos.ToDeviceRpcResponseMsg.newBuilder()
                            .setRequestId(request.getRequestId())
                            .setPayload(payload)
                            .build(),
                    TransportServiceCallback.EMPTY);
        }
    }

    private DeviceProfileRpcMethod findRpcMethod(Device targetDevice, DeviceProfile fallbackProfile, String methodName) {
        DeviceProfile profile = resolveProfileForRpc(targetDevice, fallbackProfile);
        if (StringUtils.isBlank(methodName) || profile == null || profile.getProfileData() == null) {
            return null;
        }
        List<DeviceProfileRpcMethod> methods = profile.getProfileData().getRpcMethods();
        if (methods == null || methods.isEmpty()) {
            return null;
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m != null && methodName.equals(m.getId())) {
                return m;
            }
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m != null && methodName.equals(m.getDeviceMethod())) {
                return m;
            }
        }
        return null;
    }

    private DeviceProfile resolveProfileForRpc(Device targetDevice, DeviceProfile fallbackProfile) {
        DeviceProfileId profileId = targetDevice != null && targetDevice.getDeviceProfileId() != null
                ? targetDevice.getDeviceProfileId()
                : (fallbackProfile != null ? fallbackProfile.getId() : null);
        if (profileId == null) {
            return fallbackProfile;
        }
        DeviceProfile cached = deviceProfileCache.get(profileId);
        return cached != null ? cached : fallbackProfile;
    }

    private static int resolveRpcReadTimeoutMs(TransportProtos.ToDeviceRpcRequestMsg request) {
        long remaining = remainingRpcMillis(request);
        if (remaining <= 0) {
            throw new RpcDeadlineExceededException();
        }
        return (int) Math.min(remaining, Integer.MAX_VALUE);
    }

    private static int resolveScheduledReadTimeoutMs(DeviceProfileRpcMethod rpcMethod) {
        if (rpcMethod.getTimeoutMs() != null && rpcMethod.getTimeoutMs() > 0) {
            return (int) Math.min(rpcMethod.getTimeoutMs(), Integer.MAX_VALUE);
        }
        return 10000;
    }

    private static long remainingRpcMillis(TransportProtos.ToDeviceRpcRequestMsg request) {
        if (request.getExpirationTime() <= 0) {
            return 10000L;
        }
        return request.getExpirationTime() - System.currentTimeMillis();
    }

    private static boolean isRpcExpired(TransportProtos.ToDeviceRpcRequestMsg request) {
        return request.getExpirationTime() > 0 && remainingRpcMillis(request) <= 0;
    }

    private void respondTimeout(TransportProtos.SessionInfoProto sessionInfo,
                                TransportProtos.ToDeviceRpcRequestMsg request) {
        respondError(sessionInfo, request, RPC_TIMEOUT_MESSAGE);
    }

    private static final class RpcDeadlineExceededException extends RuntimeException {
        private RpcDeadlineExceededException() {
            super(RPC_TIMEOUT_MESSAGE);
        }
    }

    private void respondManufacturerError(TransportProtos.SessionInfoProto sessionInfo,
                                          TransportProtos.ToDeviceRpcRequestMsg request,
                                          int httpStatus, String body) {
        if (request.getOneway()) {
            log.warn("[{}] HTTP outbound RPC manufacturer error (HTTP {}) ignored for one-way RPC",
                    sessionInfo.getDeviceName(), httpStatus);
            return;
        }
        var node = JacksonUtil.newObjectNode();
        node.put("httpStatus", httpStatus);
        node.put("error", extractManufacturerErrorMessage(httpStatus, body));
        if (StringUtils.isNotBlank(body)) {
            try {
                node.set("details", JacksonUtil.toJsonNode(body));
            } catch (IllegalArgumentException e) {
                node.put("details", body);
            }
        }
        respondJsonPayload(sessionInfo, request, JacksonUtil.toString(node), true);
    }

    private static String extractManufacturerErrorMessage(int httpStatus, String body) {
        if (StringUtils.isNotBlank(body)) {
            try {
                JsonNode json = JacksonUtil.toJsonNode(body);
                if (json.has("message") && !json.get("message").isNull()) {
                    return json.get("message").asText();
                }
                if (json.has("error") && !json.get("error").isNull()) {
                    return json.get("error").asText();
                }
            } catch (IllegalArgumentException ignored) {
            }
            return truncate(body);
        }
        return "HTTP status " + httpStatus;
    }

    private void respondError(TransportProtos.SessionInfoProto sessionInfo,
                              TransportProtos.ToDeviceRpcRequestMsg request, String error) {
        if (request.getOneway()) {
            return;
        }
        var node = JacksonUtil.newObjectNode();
        node.put("error", error != null ? error : "HTTP outbound RPC failed");
        respondJsonPayload(sessionInfo, request, JacksonUtil.toString(node), false);
    }

    private void respondJsonPayload(TransportProtos.SessionInfoProto sessionInfo,
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

    private static String normalizeRpcResponsePayload(String body) {
        if (StringUtils.isBlank(body)) {
            return "{}";
        }
        try {
            JacksonUtil.toJsonNode(body);
            return body;
        } catch (IllegalArgumentException ignored) {
            return JacksonUtil.toString(JacksonUtil.newObjectNode().put("response", body));
        }
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() > 256 ? s.substring(0, 256) + "..." : s;
    }
}
