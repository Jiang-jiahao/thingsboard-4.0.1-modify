/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

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
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceIdMatchStrategy;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.http.pull.session.HttpPullCollectorSessionContext;
import org.thingsboard.server.transport.http.pull.session.HttpPullTargetSession;
import org.thingsboard.server.transport.http.pull.session.ScheduledTask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
        Long frequency = profileConfig.getQueryingFrequencyMs();
        if (frequency == null || frequency <= 0) {
            return;
        }
        ScheduledTask task = new ScheduledTask();
        task.init(() -> executePoll(sessionContext), frequency, scheduler);
        sessionContext.getQueryingTasks().add(task);
    }

    public void cancelQueryingTasks(HttpPullCollectorSessionContext sessionContext) {
        sessionContext.getQueryingTasks().forEach(ScheduledTask::cancel);
        sessionContext.getQueryingTasks().clear();
        authService.invalidate(sessionContext.getDeviceId());
    }

    private ListenableFuture<Void> executePoll(HttpPullCollectorSessionContext sessionContext) {
        try {
            HttpPullDeviceProfileTransportConfiguration profile = sessionContext.getProfileTransportConfiguration();
            String pollUrl = resolvePollUrl(sessionContext);
            HttpPullAuthService.AuthRequestContext authCtx = authService.prepareAuth(
                    sessionContext.getDeviceId(), profile.getAuth(), pollUrl);

            Map<String, String> headers = new HashMap<>();
            if (profile.getPollHeaders() != null) {
                headers.putAll(profile.getPollHeaders());
            }
            if (authCtx.getHeaders() != null) {
                headers.putAll(authCtx.getHeaders());
            }
            if (!headers.containsKey("Content-Type") && StringUtils.isNotBlank(profile.getPollBody())) {
                headers.put("Content-Type", "application/json");
            }

            HttpPullHttpClient.HttpPullResponse response = httpClient.execute(HttpPullHttpClient.HttpPullRequest.builder()
                    .url(authCtx.getUrl())
                    .method(profile.getPollMethod())
                    .body(profile.getPollBody())
                    .headers(headers)
                    .queryParams(authCtx.getQueryParams())
                    .readTimeoutMs(profile.getReadTimeoutMs() != null ? profile.getReadTimeoutMs() : 10000)
                    .build());

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                String detail = "HTTP status " + response.getStatusCode() + ", body=" + truncate(response.getBody());
                log.warn("[{}] HTTP pull failed {}", sessionContext.getDeviceId(), detail);
                transportService.errorEvent(sessionContext.getTenantId(), sessionContext.getDeviceId(), "httpPullPoll",
                        new RuntimeException(detail));
                return Futures.immediateVoidFuture();
            }
            dispatchTelemetry(sessionContext, response.getBody());
        } catch (Exception e) {
            log.warn("[{}] HTTP pull poll failed", sessionContext.getDeviceId(), e);
            transportService.errorEvent(sessionContext.getTenantId(), sessionContext.getDeviceId(), "httpPullPoll", e);
        }
        return Futures.immediateVoidFuture();
    }

    private void dispatchTelemetry(HttpPullCollectorSessionContext sessionContext, String body) {
        HttpPullDeviceRoutingConfiguration routing = sessionContext.getProfileTransportConfiguration().getRouting();
        if (routing == null || routing.getRoutingMode() == HttpPullRoutingMode.SINGLE_DEVICE) {
            postPayload(sessionContext.getSessionInfo(), body, routing != null ? routing.getTelemetryPayloadKey() : "httpPullPayload");
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
            postPayload(target.getSessionInfo(), payloadJson, routing.getTelemetryPayloadKey());
        }
    }

    private void postPayload(TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, String telemetryKey) {
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

    private String resolvePollUrl(HttpPullCollectorSessionContext ctx) {
        String profilePollUrl = ctx.getProfileTransportConfiguration().getPollUrl();
        String override = ctx.getDeviceTransportConfiguration() != null
                ? ctx.getDeviceTransportConfiguration().getPollUrlOverride() : null;
        return HttpPullPollUrlResolver.resolve(profilePollUrl, override);
    }

    public static String buildMatchKey(HttpPullDeviceIdMatchStrategy strategy, TransportProtos.HttpPullRoutingTargetProto target) {
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
