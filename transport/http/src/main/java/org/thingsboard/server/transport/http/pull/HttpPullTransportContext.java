/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
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
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.transport.http.pull.service.HttpPullProtoEntityService;
import org.thingsboard.server.transport.http.pull.session.HttpPullNoOpSessionListener;
import org.thingsboard.server.transport.http.pull.session.HttpPullCollectorSessionContext;
import org.thingsboard.server.transport.http.pull.session.HttpPullTargetSession;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnExpression("'${service.type:null}'=='tb-transport' || ('${service.type:null}'=='monolith' && '${transport.api_enabled:true}'=='true' && '${transport.http.enabled}'=='true')")
@Slf4j
@RequiredArgsConstructor
public class HttpPullTransportContext extends TransportContext {

    @Getter
    private final HttpPullTransportService httpPullTransportService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final HttpPullProtoEntityService protoEntityService;

    private final Map<DeviceId, HttpPullCollectorSessionContext> collectorSessions = new ConcurrentHashMap<>();

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void fetchCollectorsAndEstablishSessions() {
        if (!isHttpPullEnabled()) {
            return;
        }
        log.info("Initializing HTTP pull collector sessions");
        int batchIndex = 0;
        int batchSize = 512;
        boolean next;
        do {
            TransportProtos.GetHttpPullDevicesResponseMsg response = protoEntityService.getHttpPullDevicesIds(batchIndex, batchSize);
            response.getIdsList().stream()
                    .map(id -> new DeviceId(UUID.fromString(id)))
                    .map(protoEntityService::getDeviceById)
                    .forEach(device -> getExecutor().execute(() -> tryEstablishCollector(device)));
            next = response.getHasNextPage();
            batchIndex++;
        } while (next);
    }

    private boolean isHttpPullEnabled() {
        return true;
    }

    private void tryEstablishCollector(Device device) {
        if (device == null) {
            return;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getTransportType() != DeviceTransportType.HTTP_PULL) {
            return;
        }
        if (!(profile.getProfileData().getTransportConfiguration() instanceof HttpPullDeviceProfileTransportConfiguration profileCfg)) {
            return;
        }
        HttpPullDeviceTransportConfiguration deviceCfg = device.getDeviceData() != null
                && device.getDeviceData().getTransportConfiguration() instanceof HttpPullDeviceTransportConfiguration h
                ? h : new HttpPullDeviceTransportConfiguration();
        if (!deviceCfg.isCollector()) {
            return;
        }
        establishCollectorSession(device, profile, profileCfg, deviceCfg);
    }

    private void establishCollectorSession(Device device, DeviceProfile profile,
                                           HttpPullDeviceProfileTransportConfiguration profileCfg,
                                           HttpPullDeviceTransportConfiguration deviceCfg) {
        DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (credentials.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            log.warn("[{}] HTTP pull collector requires ACCESS_TOKEN credentials", device.getId());
            return;
        }
        HttpPullCollectorSessionContext ctx = HttpPullCollectorSessionContext.builder()
                .tenantId(profile.getTenantId())
                .device(device)
                .deviceProfile(profile)
                .token(credentials.getCredentialsId())
                .profileTransportConfiguration(profileCfg)
                .deviceTransportConfiguration(deviceCfg)
                .transportContext(this)
                .build();
        registerCollectorAuth(ctx, msg -> {
            if (msg == null || !msg.hasDeviceInfo()) {
                return;
            }
            SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, this, UUID.randomUUID());
            transportService.registerAsyncSession(sessionInfo, HttpPullNoOpSessionListener.INSTANCE);
            ctx.setSessionInfo(sessionInfo);
            collectorSessions.put(device.getId(), ctx);
            preloadActiveTargets(ctx);
            httpPullTransportService.createQueryingTasks(ctx);
            transportService.lifecycleEvent(ctx.getTenantId(), ctx.getDeviceId(), ComponentLifecycleEvent.STARTED, true, null);
            log.info("Established HTTP pull collector session for {}", device.getId());
        });
    }

    private void preloadActiveTargets(HttpPullCollectorSessionContext collectorCtx) {
        HttpPullDeviceRoutingConfiguration routing = collectorCtx.getProfileTransportConfiguration().getRouting();
        if (routing == null || routing.getRoutingMode() != HttpPullRoutingMode.MULTI_DEVICE) {
            return;
        }
        DeviceProfileId targetProfileId = routing.getTargetDeviceProfileId() != null
                ? new DeviceProfileId(routing.getTargetDeviceProfileId())
                : collectorCtx.getDeviceProfile().getId();
        HttpPullDeviceIdMatchStrategy strategy = routing.getDeviceIdMatchStrategy() != null
                ? routing.getDeviceIdMatchStrategy() : HttpPullDeviceIdMatchStrategy.DEVICE_NAME;
        int page = 0;
        boolean next;
        do {
            TransportProtos.GetHttpPullRoutingTargetsResponseMsg resp = protoEntityService.getRoutingTargets(
                    collectorCtx.getTenantId(), targetProfileId, page, 512);
            for (TransportProtos.HttpPullRoutingTargetProto target : resp.getTargetsList()) {
                String matchKey = HttpPullTransportService.buildMatchKey(strategy, target);
                if (StringUtils.isBlank(matchKey)) {
                    continue;
                }
                DeviceId targetId = new DeviceId(new UUID(target.getDeviceIdMSB(), target.getDeviceIdLSB()));
                registerTargetSession(collectorCtx, targetId, matchKey.trim());
            }
            next = resp.getHasNextPage();
            page++;
        } while (next);
        log.info("[{}] HTTP pull active targets loaded: {}", collectorCtx.getDeviceId(), collectorCtx.getActiveTargets().size());
    }

    private void registerTargetSession(HttpPullCollectorSessionContext collectorCtx, DeviceId targetId, String matchKey) {
        DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(targetId);
        transportService.process(DeviceTransportType.HTTP_PULL,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(credentials.getCredentialsId()).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        if (!msg.hasDeviceInfo()) {
                            return;
                        }
                        SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, HttpPullTransportContext.this, UUID.randomUUID());
                        transportService.registerAsyncSession(sessionInfo, HttpPullNoOpSessionListener.INSTANCE);
                        HttpPullTargetSession targetSession = HttpPullTargetSession.builder()
                                .deviceId(targetId)
                                .matchKey(matchKey)
                                .sessionInfo(sessionInfo)
                                .build();
                        collectorCtx.getActiveTargets().put(matchKey, targetSession);
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] Failed to register HTTP pull target {}", collectorCtx.getDeviceId(), targetId, e);
                    }
                });
    }

    private void registerCollectorAuth(HttpPullCollectorSessionContext ctx,
                                       java.util.function.Consumer<ValidateDeviceCredentialsResponse> onSuccess) {
        transportService.process(DeviceTransportType.HTTP_PULL,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(ctx.getToken()).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        onSuccess.accept(msg);
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] HTTP pull collector auth failed", ctx.getDeviceId(), e);
                    }
                });
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        if (!isHttpPullEnabled()) {
            return;
        }
        Device device = event.getDevice();
        DeviceId deviceId = device.getId();
        HttpPullCollectorSessionContext existing = collectorSessions.get(deviceId);
        if (existing != null) {
            destroyCollector(existing);
            tryEstablishCollector(device);
        } else {
            tryEstablishCollector(device);
        }
    }

    private void destroyCollector(HttpPullCollectorSessionContext ctx) {
        if (ctx == null) {
            return;
        }
        ctx.getActiveTargets().values().forEach(t -> {
            if (t.getSessionInfo() != null) {
                transportService.deregisterSession(t.getSessionInfo());
            }
        });
        if (ctx.getSessionInfo() != null) {
            transportService.deregisterSession(ctx.getSessionInfo());
        }
        httpPullTransportService.cancelQueryingTasks(ctx);
        ctx.close();
        collectorSessions.remove(ctx.getDeviceId());
        transportService.lifecycleEvent(ctx.getTenantId(), ctx.getDeviceId(), ComponentLifecycleEvent.STOPPED, true, null);
    }
}
