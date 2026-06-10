/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.push;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.DefaultDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.DefaultDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceIdMatchStrategy;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.TransportContext;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.auth.SessionInfoCreator;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.transport.http.pull.HttpPullTransportService;
import org.thingsboard.server.transport.http.pull.service.HttpPullProtoEntityService;
import org.thingsboard.server.transport.http.pull.session.HttpPullNoOpSessionListener;
import org.thingsboard.server.transport.http.push.session.HttpPushGatewaySessionContext;
import org.thingsboard.server.transport.http.push.session.HttpPushTargetSession;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnExpression("'${service.type:null}'=='tb-transport' || ('${service.type:null}'=='monolith' && '${transport.api_enabled:true}'=='true' && '${transport.http.enabled}'=='true')")
@Slf4j
@RequiredArgsConstructor
public class HttpPushTransportContext extends TransportContext {

    private final HttpPushRoutingService routingService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final HttpPullProtoEntityService protoEntityService;

    private final Map<DeviceId, HttpPushGatewaySessionContext> gatewayContexts = new ConcurrentHashMap<>();

    public HttpPushGatewaySessionContext getOrCreateGatewayContext(DeviceId gatewayDeviceId, SessionInfoProto gatewaySessionInfo) {
        HttpPushGatewaySessionContext existing = gatewayContexts.get(gatewayDeviceId);
        if (existing != null) {
            existing.setGatewaySessionInfo(gatewaySessionInfo);
            return existing;
        }
        Device device = protoEntityService.getDeviceById(gatewayDeviceId);
        if (device == null) {
            return null;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getTransportType() != DeviceTransportType.DEFAULT) {
            return null;
        }
        if (!(profile.getProfileData().getTransportConfiguration() instanceof DefaultDeviceProfileTransportConfiguration profileCfg)) {
            return null;
        }
        DefaultDeviceTransportConfiguration deviceCfg = device.getDeviceData() != null
                && device.getDeviceData().getTransportConfiguration() instanceof DefaultDeviceTransportConfiguration d
                ? d : new DefaultDeviceTransportConfiguration();
        if (!deviceCfg.isGateway()) {
            return null;
        }
        HttpPullDeviceRoutingConfiguration routing = profileCfg.getRouting();
        if (routing == null) {
            return null;
        }
        HttpPushGatewaySessionContext ctx = HttpPushGatewaySessionContext.builder()
                .tenantId(profile.getTenantId())
                .device(device)
                .deviceProfile(profile)
                .gatewaySessionInfo(gatewaySessionInfo)
                .profileTransportConfiguration(profileCfg)
                .deviceTransportConfiguration(deviceCfg)
                .routingService(routingService)
                .build();
        gatewayContexts.put(gatewayDeviceId, ctx);
        if (routing.getRoutingMode() == HttpPullRoutingMode.MULTI_DEVICE) {
            preloadActiveTargets(ctx);
        }
        return ctx;
    }

    private void preloadActiveTargets(HttpPushGatewaySessionContext gatewayCtx) {
        HttpPullDeviceRoutingConfiguration routing = gatewayCtx.getProfileTransportConfiguration().getRouting();
        if (routing == null || routing.getRoutingMode() != HttpPullRoutingMode.MULTI_DEVICE) {
            return;
        }
        gatewayCtx.clearTargets();
        DeviceProfileId targetProfileId = routing.getTargetDeviceProfileId() != null
                ? new DeviceProfileId(routing.getTargetDeviceProfileId())
                : gatewayCtx.getDeviceProfile().getId();
        HttpPullDeviceIdMatchStrategy strategy = routing.getDeviceIdMatchStrategy() != null
                ? routing.getDeviceIdMatchStrategy() : HttpPullDeviceIdMatchStrategy.DEVICE_NAME;
        int page = 0;
        boolean next;
        do {
            TransportProtos.GetHttpPullRoutingTargetsResponseMsg resp = protoEntityService.getRoutingTargets(
                    gatewayCtx.getTenantId(), targetProfileId, page, 512);
            for (TransportProtos.HttpPullRoutingTargetProto target : resp.getTargetsList()) {
                String matchKey = HttpPullTransportService.buildMatchKey(strategy, target);
                if (StringUtils.isBlank(matchKey)) {
                    continue;
                }
                DeviceId targetId = new DeviceId(new UUID(target.getDeviceIdMSB(), target.getDeviceIdLSB()));
                registerTargetSession(gatewayCtx, targetId, matchKey.trim());
            }
            next = resp.getHasNextPage();
            page++;
        } while (next);
        log.info("[{}] HTTP push active targets loaded: {}", gatewayCtx.getDeviceId(), gatewayCtx.getActiveTargets().size());
    }

    private void registerTargetSession(HttpPushGatewaySessionContext gatewayCtx, DeviceId targetId, String matchKey) {
        if (gatewayCtx.getActiveTargets().containsKey(matchKey)) {
            return;
        }
        DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(targetId);
        transportService.process(DeviceTransportType.DEFAULT,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(credentials.getCredentialsId()).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        if (!msg.hasDeviceInfo()) {
                            return;
                        }
                        SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, HttpPushTransportContext.this, UUID.randomUUID());
                        transportService.registerAsyncSession(sessionInfo, HttpPullNoOpSessionListener.INSTANCE);
                        HttpPushTargetSession targetSession = HttpPushTargetSession.builder()
                                .deviceId(targetId)
                                .matchKey(matchKey)
                                .sessionInfo(sessionInfo)
                                .build();
                        gatewayCtx.getActiveTargets().put(matchKey, targetSession);
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] Failed to register HTTP push target {}", gatewayCtx.getDeviceId(), targetId, e);
                    }
                });
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        Device device = event.getDevice();
        DeviceId deviceId = device.getId();
        HttpPushGatewaySessionContext gatewayCtx = gatewayContexts.get(deviceId);
        if (gatewayCtx != null) {
            preloadActiveTargets(gatewayCtx);
            return;
        }
        for (HttpPushGatewaySessionContext ctx : gatewayContexts.values()) {
            if (ctx.getDeviceProfile().getId().equals(device.getDeviceProfileId())) {
                preloadActiveTargets(ctx);
            }
        }
    }

    public void invalidateGateway(DeviceId gatewayDeviceId) {
        HttpPushGatewaySessionContext removed = gatewayContexts.remove(gatewayDeviceId);
        if (removed != null) {
            removed.clearTargets();
        }
    }
}
