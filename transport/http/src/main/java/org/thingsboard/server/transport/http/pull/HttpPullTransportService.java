/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.http.HttpPullPollRequest;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.transport.http.pull.session.HttpPullCollectorSessionContext;
import org.thingsboard.server.transport.http.pull.session.HttpPullTargetSession;
import org.thingsboard.server.transport.http.pull.session.ScheduledTask;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class HttpPullTransportService {

    private final TransportService transportService;
    private final HttpPullAuthService authService;
    private ListeningScheduledExecutorService scheduler;
    private HttpPullHttpClient httpClient;

    @Value("${transport.http.pull.scheduler_thread_pool_size:4}")
    private int schedulerThreadPoolSize;

    @PostConstruct
    public void init() {
        scheduler = MoreExecutors.listeningDecorator(
                ThingsBoardExecutors.newScheduledThreadPool(schedulerThreadPoolSize, "http-pull-querying"));
        httpClient = new HttpPullHttpClient(10000);
    }

    @PreDestroy
    public void destroy() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    public void createQueryingTasks(HttpPullCollectorSessionContext sessionContext) {
        HttpPullDeviceProfileTransportConfiguration profileConfig = sessionContext.getProfileTransportConfiguration();
        List<HttpPullPollRequest> requests = profileConfig.effectivePollRequests();
        for (HttpPullPollRequest pollRequest : requests) {
            long frequency = profileConfig.resolveQueryingFrequencyMs(pollRequest);
            ScheduledTask task = new ScheduledTask();
            task.init(() -> executePoll(sessionContext, pollRequest), frequency, scheduler);
            sessionContext.getQueryingTasks().add(task);
        }
    }

    public void cancelQueryingTasks(HttpPullCollectorSessionContext sessionContext) {
        sessionContext.getQueryingTasks().forEach(ScheduledTask::cancel);
        sessionContext.getQueryingTasks().clear();
        authService.invalidate(sessionContext.getDeviceId());
    }

    private ListenableFuture<Void> executePoll(HttpPullCollectorSessionContext sessionContext, HttpPullPollRequest pollRequest) {
        try {
            HttpPullDeviceProfileTransportConfiguration profile = sessionContext.getProfileTransportConfiguration();
            String pollUrl = resolvePollUrl(sessionContext, pollRequest);
            boolean requiresAuth = pollRequest.isRequiresAuth(profile.getAuth());
            HttpPullAuthService.AuthRequestContext authCtx = authService.prepareAuth(
                    sessionContext.getDeviceId(), profile.getAuth(), pollUrl, requiresAuth);

            HttpPullHttpClient.HttpPullResponse response = executeHttpRequest(sessionContext, pollRequest, profile, authCtx);

            if (response.getStatusCode() == 401 && requiresAuth) {
                log.info("[{}] HTTP pull [{}] 401, refreshing login token", sessionContext.getDeviceId(), pollRequest.getName());
                authService.invalidate(sessionContext.getDeviceId());
                authCtx = authService.prepareAuth(sessionContext.getDeviceId(), profile.getAuth(), pollUrl, true);
                response = executeHttpRequest(sessionContext, pollRequest, profile, authCtx);
            }

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                String detail = "HTTP status " + response.getStatusCode() + ", body=" + truncate(response.getBody());
                log.warn("[{}] HTTP pull [{}] failed {}", sessionContext.getDeviceId(), pollRequest.getName(), detail);
                transportService.errorEvent(sessionContext.getTenantId(), sessionContext.getDeviceId(), "httpPullPoll",
                        new RuntimeException(detail));
                return Futures.immediateVoidFuture();
            }
            dispatchResponse(sessionContext, pollRequest, response.getBody());
        } catch (Exception e) {
            log.warn("[{}] HTTP pull [{}] failed", sessionContext.getDeviceId(), pollRequest.getName(), e);
            transportService.errorEvent(sessionContext.getTenantId(), sessionContext.getDeviceId(), "httpPullPoll", e);
        }
        return Futures.immediateVoidFuture();
    }

    private HttpPullHttpClient.HttpPullResponse executeHttpRequest(HttpPullCollectorSessionContext sessionContext,
                                                                   HttpPullPollRequest pollRequest,
                                                                   HttpPullDeviceProfileTransportConfiguration profile,
                                                                   HttpPullAuthService.AuthRequestContext authCtx) throws Exception {
        Map<String, String> headers = new HashMap<>();
        if (pollRequest.getPollHeaders() != null) {
            headers.putAll(pollRequest.getPollHeaders());
        }
        if (authCtx.getHeaders() != null) {
            headers.putAll(authCtx.getHeaders());
        }
        String body = pollRequest.getPollBody();
        if (!headers.containsKey("Content-Type") && StringUtils.isNotBlank(body)) {
            headers.put("Content-Type", "application/json");
        }
        return httpClient.execute(HttpPullHttpClient.HttpPullRequest.builder()
                .url(authCtx.getUrl())
                .method(pollRequest.getPollMethod())
                .body(body)
                .headers(headers)
                .queryParams(authCtx.getQueryParams())
                .readTimeoutMs(profile.getReadTimeoutMs() != null ? profile.getReadTimeoutMs() : 10000)
                .build());
    }

    private void dispatchResponse(HttpPullCollectorSessionContext sessionContext, HttpPullPollRequest pollRequest, String body) {
        HttpPullDeviceRoutingConfiguration routing = sessionContext.getProfileTransportConfiguration().resolveRouting(pollRequest);
        HttpPullPollDataType dataType = pollRequest.getDataType() != null
                ? pollRequest.getDataType() : HttpPullPollDataType.TELEMETRY;
        if (dataType == HttpPullPollDataType.TELEMETRY) {
            dispatchTelemetry(sessionContext, body, routing);
        } else {
            boolean shared = dataType == HttpPullPollDataType.SHARED_ATTRIBUTES;
            dispatchAttributes(sessionContext, body, shared, routing);
        }
    }

    private void dispatchTelemetry(HttpPullCollectorSessionContext sessionContext, String body,
                                   HttpPullDeviceRoutingConfiguration routing) {
        if (!HttpPullRoutingHelper.shouldRouteToMultipleDevices(routing, body)) {
            postTelemetry(sessionContext.getSessionInfo(), body,
                    routing != null ? routing.getTelemetryPayloadKey() : "httpPullPayload");
            return;
        }
        List<Object> elements = HttpPullJsonHelper.readArrayElements(body, routing.getResponseArrayJsonPath());
        for (Object element : elements) {
            String externalId = HttpPullJsonHelper.readDeviceId(element, routing.getDeviceIdJsonPath());
            if (StringUtils.isBlank(externalId)) {
                continue;
            }
            HttpPullTargetSession target = sessionContext.getActiveTargets().get(externalId.trim());
            if (target == null || target.getSessionInfo() == null) {
                log.debug("[{}] No active target for external device id [{}]", sessionContext.getDeviceId(), externalId);
                continue;
            }
            String payloadJson = HttpPullJsonHelper.elementToJsonString(element);
            postTelemetry(target.getSessionInfo(), payloadJson, routing.getTelemetryPayloadKey());
        }
    }

    private void dispatchAttributes(HttpPullCollectorSessionContext sessionContext, String body, boolean shared,
                                    HttpPullDeviceRoutingConfiguration routing) {
        if (!HttpPullRoutingHelper.shouldRouteToMultipleDevices(routing, body)) {
            postAttributes(sessionContext.getSessionInfo(), body, shared);
            return;
        }
        List<Object> elements = HttpPullJsonHelper.readArrayElements(body, routing.getResponseArrayJsonPath());
        for (Object element : elements) {
            String externalId = HttpPullJsonHelper.readDeviceId(element, routing.getDeviceIdJsonPath());
            if (StringUtils.isBlank(externalId)) {
                continue;
            }
            HttpPullTargetSession target = sessionContext.getActiveTargets().get(externalId.trim());
            if (target == null || target.getSessionInfo() == null) {
                continue;
            }
            String payloadJson = HttpPullJsonHelper.elementToJsonString(element);
            postAttributes(target.getSessionInfo(), payloadJson, shared);
        }
    }

    private void postTelemetry(TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, String telemetryKey) {
        String key = StringUtils.isNotBlank(telemetryKey) ? telemetryKey : "httpPullPayload";
        JsonObject wrapper = new JsonObject();
        try {
            wrapper.add(key, JsonParser.parseString(jsonPayload));
        } catch (Exception e) {
            wrapper.addProperty(key, jsonPayload);
        }
        TransportProtos.PostTelemetryMsg msg = JsonConverter.convertToTelemetryProto(wrapper);
        transportService.process(sessionInfo, msg, null);
    }

    private void postAttributes(TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, boolean shared) {
        JsonElement parsed = JsonParser.parseString(jsonPayload);
        if (!parsed.isJsonObject()) {
            log.warn("HTTP pull attributes response is not a JSON object, skipping");
            return;
        }
        TransportProtos.PostAttributeMsg.Builder builder = JsonConverter.convertToAttributesProto(parsed).toBuilder();
        builder.setShared(shared);
        transportService.process(sessionInfo, builder.build(), null);
    }

    private String resolvePollUrl(HttpPullCollectorSessionContext ctx, HttpPullPollRequest pollRequest) {
        String override = ctx.getDeviceTransportConfiguration() != null
                ? ctx.getDeviceTransportConfiguration().getPollUrlOverride() : null;
        return HttpPullPollUrlResolver.resolve(pollRequest.getPollUrl(), override);
    }

    public static String buildMatchKey(org.thingsboard.server.common.data.transport.http.HttpPullDeviceIdMatchStrategy strategy,
                                       TransportProtos.HttpPullRoutingTargetProto target) {
        return switch (strategy) {
            case DEVICE_LABEL -> target.getLabel();
            case EXTERNAL_DEVICE_ID -> target.getExternalDeviceId();
            default -> target.getName();
        };
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() > 256 ? s.substring(0, 256) + "..." : s;
    }
}
