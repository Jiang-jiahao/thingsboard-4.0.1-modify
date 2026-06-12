/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.rpc.RpcStatus;
import org.thingsboard.server.common.data.transport.http.HttpPullAuthConfiguration;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.http.pull.session.HttpPullCollectorSessionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class HttpPullRpcService {

    private static final String RPC_TIMEOUT_MESSAGE = "HTTP outbound RPC timed out";

    private final HttpPullAuthService authService;
    private final TransportService transportService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private HttpPullHttpClient httpClient;

    @jakarta.annotation.PostConstruct
    public void init() {
        httpClient = new HttpPullHttpClient(10000);
    }

    public void onToDeviceRpcRequest(HttpPullCollectorSessionContext collectorCtx, Device targetDevice,
                                     HttpPullDeviceTransportConfiguration targetDeviceCfg,
                                     TransportProtos.SessionInfoProto sessionInfo,
                                     TransportProtos.ToDeviceRpcRequestMsg request) {
        DeviceProfileRpcMethod rpcMethod = findRpcMethod(targetDevice, collectorCtx, request.getMethodName());
        if (rpcMethod == null) {
            log.warn("[{}] HTTP pull RPC method not found: {}", collectorCtx.getDeviceId(), request.getMethodName());
            respondError(sessionInfo, request, "RPC method not found: " + request.getMethodName());
            return;
        }
        if (rpcMethod.getBindingType() != DeviceProfileRpcBindingType.HTTP_OUTBOUND) {
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
                executeOutboundRpc(collectorCtx, targetDevice, targetDeviceCfg, sessionInfo, request, rpcMethod);
            } catch (Exception e) {
                log.warn("[{}] HTTP outbound RPC [{}] failed", targetDevice != null ? targetDevice.getId() : collectorCtx.getDeviceId(),
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

    private DeviceProfileRpcMethod findRpcMethod(Device targetDevice, HttpPullCollectorSessionContext collectorCtx,
                                                 String methodName) {
        DeviceProfile profile = resolveProfileForRpc(targetDevice, collectorCtx);
        if (StringUtils.isBlank(methodName) || profile == null || profile.getProfileData() == null) {
            return null;
        }
        List<DeviceProfileRpcMethod> methods = profile.getProfileData().getRpcMethods();
        if (methods == null || methods.isEmpty()) {
            return null;
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m == null || StringUtils.isBlank(m.getId())) {
                continue;
            }
            if (methodName.equals(m.getId())) {
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

    private DeviceProfile resolveProfileForRpc(Device targetDevice, HttpPullCollectorSessionContext collectorCtx) {
        DeviceProfileId profileId = targetDevice != null && targetDevice.getDeviceProfileId() != null
                ? targetDevice.getDeviceProfileId()
                : collectorCtx.getDeviceProfile().getId();
        DeviceProfile cached = deviceProfileCache.get(profileId);
        return cached != null ? cached : collectorCtx.getDeviceProfile();
    }

    private void executeOutboundRpc(HttpPullCollectorSessionContext collectorCtx, Device targetDevice,
                                    HttpPullDeviceTransportConfiguration targetDeviceCfg,
                                    TransportProtos.SessionInfoProto sessionInfo,
                                    TransportProtos.ToDeviceRpcRequestMsg request,
                                    DeviceProfileRpcMethod rpcMethod) throws Exception {
        HttpPullDeviceProfileTransportConfiguration profile = collectorCtx.getProfileTransportConfiguration();
        String paramsJson = request.getParams() != null ? request.getParams() : "{}";
        String urlOverride = targetDeviceCfg != null ? targetDeviceCfg.getPollUrlOverride() : null;
        if (StringUtils.isBlank(urlOverride) && collectorCtx.getDeviceTransportConfiguration() != null) {
            urlOverride = collectorCtx.getDeviceTransportConfiguration().getPollUrlOverride();
        }
        String url = HttpPullPollUrlResolver.resolve(rpcMethod.getHttpUrl(), urlOverride);
        url = HttpPullTemplateResolver.resolve(url, targetDevice, targetDeviceCfg, paramsJson);

        HttpPullAuthConfiguration auth = profile.getAuth();
        boolean requiresAuth = rpcMethod.getRequiresAuth() != null
                ? rpcMethod.getRequiresAuth()
                : auth != null && auth.getAuthType() != null
                && auth.getAuthType() != org.thingsboard.server.common.data.transport.http.HttpPullAuthType.NONE;

        int readTimeoutMs = resolveRpcReadTimeoutMs(request);
        HttpPullAuthService.AuthRequestContext authCtx = authService.prepareAuth(
                collectorCtx.getDeviceId(), auth, url, requiresAuth, urlOverride, readTimeoutMs);

        Map<String, String> headers = new HashMap<>();
        if (rpcMethod.getHttpHeaders() != null) {
            headers.putAll(HttpPullTemplateResolver.resolveHeaders(
                    rpcMethod.getHttpHeaders(), targetDevice, targetDeviceCfg, paramsJson));
        }
        if (authCtx.getHeaders() != null) {
            headers.putAll(authCtx.getHeaders());
        }

        String body = HttpPullTemplateResolver.resolve(
                rpcMethod.getHttpBody(), targetDevice, targetDeviceCfg, paramsJson);
        if (!headers.containsKey("Content-Type") && StringUtils.isNotBlank(body)) {
            headers.put("Content-Type", "application/json");
        }

        log.info("[{}] HTTP outbound RPC [{}] {} {} body={}",
                collectorCtx.getDeviceId(), rpcMethod.getId(), rpcMethod.getHttpMethod(), url, truncate(body));

        HttpPullHttpClient.HttpPullResponse response = executeHttp(request, rpcMethod, authCtx, headers, body);

        if (response.getStatusCode() == 401 && requiresAuth) {
            log.info("[{}] HTTP outbound RPC [{}] 401, refreshing login token",
                    collectorCtx.getDeviceId(), rpcMethod.getId());
            authService.invalidate(collectorCtx.getDeviceId());
            readTimeoutMs = resolveRpcReadTimeoutMs(request);
            authCtx = authService.prepareAuth(collectorCtx.getDeviceId(), auth, url, true, urlOverride, readTimeoutMs);
            headers = new HashMap<>();
            if (rpcMethod.getHttpHeaders() != null) {
                headers.putAll(HttpPullTemplateResolver.resolveHeaders(
                        rpcMethod.getHttpHeaders(), targetDevice, targetDeviceCfg, paramsJson));
            }
            if (authCtx.getHeaders() != null) {
                headers.putAll(authCtx.getHeaders());
            }
            response = executeHttp(request, rpcMethod, authCtx, headers, body);
        }

        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
            respondManufacturerError(sessionInfo, request, response.getStatusCode(), response.getBody());
            return;
        }

        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().activateHttpPullDeviceSession(sessionInfo, collectorCtx.getDeviceId());
        }
        transportService.process(sessionInfo, request, RpcStatus.DELIVERED, TransportServiceCallback.EMPTY);
        if (!request.getOneway()) {
            String payload = normalizeRpcResponsePayload(response.getBody());
            transportService.process(sessionInfo,
                    TransportProtos.ToDeviceRpcResponseMsg.newBuilder()
                            .setRequestId(request.getRequestId())
                            .setPayload(payload)
                            .build(),
                    TransportServiceCallback.EMPTY);
        }
    }

    private HttpPullHttpClient.HttpPullResponse executeHttp(TransportProtos.ToDeviceRpcRequestMsg request,
                                                            DeviceProfileRpcMethod rpcMethod,
                                                            HttpPullAuthService.AuthRequestContext authCtx,
                                                            Map<String, String> headers,
                                                            String body) throws Exception {
        return httpClient.execute(HttpPullHttpClient.HttpPullRequest.builder()
                .url(authCtx.getUrl())
                .method(rpcMethod.getHttpMethod())
                .body(body)
                .headers(headers)
                .queryParams(authCtx.getQueryParams())
                .readTimeoutMs(resolveRpcReadTimeoutMs(request))
                .build());
    }

    /**
     * 与 REST RPC {@code expirationTime} 对齐：HTTP 读超时 = 到期剩余时间（与设备 Actor / 规则引擎一致）。
     */
    private static int resolveRpcReadTimeoutMs(TransportProtos.ToDeviceRpcRequestMsg request) {
        long remaining = remainingRpcMillis(request);
        if (remaining <= 0) {
            throw new RpcDeadlineExceededException();
        }
        return (int) Math.min(remaining, Integer.MAX_VALUE);
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

    /**
     * 厂家 HTTP 非 2xx：以 JSON payload 返回，避免 REST 层将纯文本 error 当响应体解析导致 406。
     */
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
