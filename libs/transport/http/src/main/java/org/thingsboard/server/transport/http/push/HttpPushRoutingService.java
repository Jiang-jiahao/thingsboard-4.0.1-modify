/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.push;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.http.pull.HttpPullJsonHelper;
import org.thingsboard.server.transport.http.push.session.HttpPushGatewaySessionContext;
import org.thingsboard.server.transport.http.push.session.HttpPushTargetSession;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class HttpPushRoutingService {

    private final TransportService transportService;

    public boolean shouldRoute(HttpPushGatewaySessionContext ctx) {
        if (ctx == null || ctx.getProfileTransportConfiguration() == null) {
            return false;
        }
        HttpPullDeviceRoutingConfiguration routing = ctx.getProfileTransportConfiguration().getRouting();
        return routing != null && routing.getRoutingMode() == HttpPullRoutingMode.MULTI_DEVICE;
    }

    public void dispatchTelemetry(HttpPushGatewaySessionContext ctx, String body,
                                  TransportServiceCallback<Void> responseCallback) {
        HttpPullDeviceRoutingConfiguration routing = ctx.getProfileTransportConfiguration().getRouting();
        if (routing == null || routing.getRoutingMode() == HttpPullRoutingMode.SINGLE_DEVICE) {
            postPayload(ctx.getGatewaySessionInfo(), body,
                    routing != null ? routing.getTelemetryPayloadKey() : "httpPushPayload", responseCallback);
            return;
        }
        List<Object> elements = HttpPullJsonHelper.readArrayElements(body, routing.getResponseArrayJsonPath());
        int routed = 0;
        for (Object element : elements) {
            String externalId = HttpPullJsonHelper.readDeviceId(element, routing.getDeviceIdJsonPath());
            if (StringUtils.isBlank(externalId)) {
                continue;
            }
            HttpPushTargetSession target = ctx.getActiveTargets().get(externalId.trim());
            if (target == null || target.getSessionInfo() == null) {
                log.debug("[{}] No active HTTP push target for external device id [{}]", ctx.getDeviceId(), externalId);
                continue;
            }
            String payloadJson = HttpPullJsonHelper.elementToJsonString(element);
            postPayload(target.getSessionInfo(), payloadJson, routing.getTelemetryPayloadKey(), null);
            routed++;
        }
        if (responseCallback != null) {
            if (routed > 0 || elements.isEmpty()) {
                responseCallback.onSuccess(null);
            } else {
                responseCallback.onError(new IllegalStateException("No matching HTTP push target devices for payload"));
            }
        }
    }

    private void postPayload(TransportProtos.SessionInfoProto sessionInfo, String jsonPayload, String telemetryKey,
                             TransportServiceCallback<Void> responseCallback) {
        String key = StringUtils.isNotBlank(telemetryKey) ? telemetryKey : "httpPushPayload";
        JsonObject wrapper = new JsonObject();
        try {
            wrapper.add(key, JsonParser.parseString(jsonPayload));
        } catch (Exception e) {
            wrapper.addProperty(key, jsonPayload);
        }
        TransportProtos.PostTelemetryMsg msg = JsonConverter.convertToTelemetryProto(wrapper);
        transportService.process(sessionInfo, msg, responseCallback);
    }
}
