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
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.http.HttpPullPollRequest;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.transport.http.pull.session.HttpPullCollectorSessionContext;
import org.thingsboard.server.transport.http.pull.session.HttpPullPollFailureTracker;
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

    void setHttpClient(HttpPullHttpClient httpClient) {
        this.httpClient = httpClient;
    }

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

    ListenableFuture<Void> executePoll(HttpPullCollectorSessionContext sessionContext, HttpPullPollRequest pollRequest) {
        if (sessionContext.getTransportContext() != null
                && !sessionContext.getTransportContext().isManagedByCurrentTransport(sessionContext.getDeviceId().getId())) {
            return Futures.immediateVoidFuture();
        }
        try {
            HttpPullDeviceProfileTransportConfiguration profile = sessionContext.getProfileTransportConfiguration();
            String pollUrl = resolvePollUrl(sessionContext, pollRequest);
            String urlOverride = resolvePollUrlOverride(sessionContext);
            boolean requiresAuth = pollRequest.isRequiresAuth(profile.getAuth());
            HttpPullAuthService.AuthRequestContext authCtx = authService.prepareAuth(
                    sessionContext.getDeviceId(), profile.getAuth(), pollUrl, requiresAuth, urlOverride);

            HttpPullHttpClient.HttpPullResponse response = executeHttpRequest(sessionContext, pollRequest, profile, authCtx);

            if (response.getStatusCode() == 401 && requiresAuth) {
                log.info("[{}] HTTP pull [{}] 401, refreshing login token", sessionContext.getDeviceId(), pollRequest.getName());
                authService.invalidate(sessionContext.getDeviceId());
                authCtx = authService.prepareAuth(sessionContext.getDeviceId(), profile.getAuth(), pollUrl, true, urlOverride);
                response = executeHttpRequest(sessionContext, pollRequest, profile, authCtx);
            }

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                String detail = "HTTP status " + response.getStatusCode() + ", body=" + truncate(response.getBody());
                reportPollFailure(sessionContext, pollRequest, detail, null);
                return Futures.immediateVoidFuture();
            }
            reportPollSuccess(sessionContext, pollRequest);
            dispatchResponse(sessionContext, pollRequest, response.getBody());
        } catch (Exception e) {
            reportPollFailure(sessionContext, pollRequest,
                    e.getClass().getSimpleName() + ": " + e.getMessage(), e);
        }
        return Futures.immediateVoidFuture();
    }

    /**
     * 轮询失败按「错误签名」抑制输出：厂家不可达/鉴权失效/路径配错都会按轮询间隔持续失败，
     * 每次都打 WARN + 堆栈会让日志迅速刷满。首次与错误变化时完整输出，同一种错误持续期间
     * 只在窗口内打一条摘要，其余降到 DEBUG；errorEvent 与之一致，避免下游事件/规则链被同样刷屏。
     */
    private void reportPollFailure(HttpPullCollectorSessionContext sessionContext, HttpPullPollRequest pollRequest,
                                   String detail, Throwable error) {
        HttpPullPollFailureTracker tracker = sessionContext.getPollFailures()
                .computeIfAbsent(pollRequest.getId(), id -> new HttpPullPollFailureTracker());
        HttpPullPollFailureTracker.Report report = tracker.onFailure(detail, System.currentTimeMillis());
        String deviceId = String.valueOf(sessionContext.getDeviceId());
        if (report.firstOrChanged()) {
            if (error != null) {
                log.warn("[{}] HTTP pull [{}] failed", deviceId, pollRequest.getName(), error);
            } else {
                log.warn("[{}] HTTP pull [{}] failed {}", deviceId, pollRequest.getName(), detail);
            }
        } else if (report.reported()) {
            log.warn("[{}] HTTP pull [{}] still failing ({} consecutive): {}",
                    deviceId, pollRequest.getName(), report.consecutive(), detail);
        } else {
            log.debug("[{}] HTTP pull [{}] failed ({} consecutive): {}",
                    deviceId, pollRequest.getName(), report.consecutive(), detail);
        }
        if (report.reported()) {
            transportService.errorEvent(sessionContext.getTenantId(), sessionContext.getDeviceId(), "httpPullPoll",
                    error != null ? error : new RuntimeException(detail));
        }
    }

    private void reportPollSuccess(HttpPullCollectorSessionContext sessionContext, HttpPullPollRequest pollRequest) {
        HttpPullPollFailureTracker tracker = sessionContext.getPollFailures().get(pollRequest.getId());
        int recovered = tracker != null ? tracker.reset() : 0;
        if (recovered > 0) {
            log.info("[{}] HTTP pull [{}] recovered after {} consecutive failures",
                    sessionContext.getDeviceId(), pollRequest.getName(), recovered);
        }
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

    void dispatchResponse(HttpPullCollectorSessionContext sessionContext, HttpPullPollRequest pollRequest, String body) {
        HttpPullPollDataType dataType = pollRequest.getDataType() != null
                ? pollRequest.getDataType() : HttpPullPollDataType.TELEMETRY;
        if (dataType == HttpPullPollDataType.TELEMETRY) {
            postTelemetry(sessionContext, sessionContext.getSessionInfo(), body, pollRequest.resolveTelemetryPayloadKey());
        } else {
            postAttributes(sessionContext, sessionContext.getSessionInfo(), body,
                    dataType == HttpPullPollDataType.SHARED_ATTRIBUTES);
        }
    }

    private void postTelemetry(HttpPullCollectorSessionContext collectorCtx,
                               TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, String telemetryKey) {
        if (sessionInfo == null) {
            log.warn("[{}] Skip telemetry: HTTP pull session is not ready", collectorCtx.getDeviceId());
            return;
        }
        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().activateHttpPullDeviceSession(sessionInfo, collectorCtx.getDeviceId());
        }
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

    private void postAttributes(HttpPullCollectorSessionContext collectorCtx,
                                TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, boolean shared) {
        if (sessionInfo == null) {
            log.warn("[{}] Skip attributes: HTTP pull session is not ready", collectorCtx.getDeviceId());
            return;
        }
        if (collectorCtx.getTransportContext() != null) {
            collectorCtx.getTransportContext().activateHttpPullDeviceSession(sessionInfo, collectorCtx.getDeviceId());
        }
        JsonElement parsed;
        try {
            parsed = JsonParser.parseString(jsonPayload);
        } catch (Exception e) {
            log.warn("[{}] HTTP pull attributes payload is not valid JSON, skipping", collectorCtx.getDeviceId());
            return;
        }
        if (!parsed.isJsonObject()) {
            log.warn("[{}] HTTP pull attributes response is not a JSON object, skipping", collectorCtx.getDeviceId());
            return;
        }
        TransportProtos.PostAttributeMsg.Builder builder = JsonConverter.convertToAttributesProto(parsed).toBuilder();
        builder.setShared(shared);
        transportService.process(sessionInfo, builder.build(), null);
    }

    private String resolvePollUrl(HttpPullCollectorSessionContext ctx, HttpPullPollRequest pollRequest) {
        return HttpPullPollUrlResolver.resolve(pollRequest.getPollUrl(), resolvePollUrlOverride(ctx));
    }

    private String resolvePollUrlOverride(HttpPullCollectorSessionContext ctx) {
        return ctx.getDeviceTransportConfiguration() != null
                ? ctx.getDeviceTransportConfiguration().getPollUrlOverride() : null;
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
