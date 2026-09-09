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
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;
import org.thingsboard.server.common.transport.DeviceDeletedEvent;
import org.thingsboard.server.common.transport.DeviceProfileUpdatedEvent;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.TransportContext;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.SessionMsgListener;
import org.thingsboard.server.common.transport.service.DefaultTransportService;
import org.thingsboard.server.common.transport.auth.SessionInfoCreator;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.common.util.AfterStartUp;
import org.thingsboard.server.transport.http.HttpTransportBalancingService;
import org.thingsboard.server.transport.http.event.HttpTransportListChangedEvent;
import org.thingsboard.server.transport.http.pull.service.HttpPullProtoEntityService;
import org.thingsboard.server.transport.http.pull.session.HttpPullCollectorSessionContext;
import org.thingsboard.server.transport.http.pull.session.HttpPullRpcSessionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.http.enabled:true}'=='true'")
@Slf4j
@RequiredArgsConstructor
public class HttpPullTransportContext extends TransportContext {

    @Getter
    private final HttpPullTransportService httpPullTransportService;
    private final HttpPullRpcService httpPullRpcService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final HttpPullProtoEntityService protoEntityService;
    private final HttpTransportBalancingService balancingService;

    private final Map<DeviceId, HttpPullCollectorSessionContext> collectorSessions = new ConcurrentHashMap<>();
    private final Set<DeviceId> allHttpPullDeviceIds = ConcurrentHashMap.newKeySet();
    /** 正在异步鉴权/建会话的设备，避免 reconcile 与 DeviceUpdated 并发导致重复 STARTED */
    private final Set<DeviceId> establishingCollectors = ConcurrentHashMap.newKeySet();
    /** 已向 Core 上报 OPEN/RPC 订阅的会话；仅在实际连通并同步数据后才加入 */
    private final Set<UUID> activatedTransportSessions = ConcurrentHashMap.newKeySet();

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void fetchCollectorsAndEstablishSessions() {
        if (!isHttpPullEnabled()) {
            return;
        }
        log.info("Initializing HTTP pull collector sessions");
        reconcileCollectors();
        getScheduler().schedule(this::reconcileCollectors, 5, TimeUnit.SECONDS);
        getScheduler().schedule(this::reconcileCollectors, 15, TimeUnit.SECONDS);
    }

    private synchronized void reconcileCollectors() {
        if (!isHttpPullEnabled()) {
            return;
        }
        try {
            reloadAllHttpPullDeviceIds();
            int managed = 0;
            for (DeviceId deviceId : allHttpPullDeviceIds) {
                if (balancingService.isManagedByCurrentTransport(deviceId.getId())) {
                    managed++;
                    if (!collectorSessions.containsKey(deviceId) && !establishingCollectors.contains(deviceId)) {
                        Device device = protoEntityService.getDeviceById(deviceId);
                        if (device != null) {
                            getExecutor().execute(() -> tryEstablishCollector(device));
                        }
                    }
                } else {
                    HttpPullCollectorSessionContext ctx = collectorSessions.get(deviceId);
                    if (ctx != null) {
                        log.info("[{}] HTTP pull collector is not managed by current node anymore", deviceId);
                        destroyCollector(ctx);
                    }
                }
            }
            log.info("HTTP pull reconcile: devices={}, managed by this node={}", allHttpPullDeviceIds.size(), managed);
            log.info("HTTP pull collectors owned by this node: {}", collectorSessions.keySet());
        } catch (Exception e) {
            log.warn("Failed to reconcile HTTP pull collectors", e);
        }
    }

    private void reloadAllHttpPullDeviceIds() {
        Set<DeviceId> loaded = ConcurrentHashMap.newKeySet();
        int batchIndex = 0;
        int batchSize = 512;
        boolean next;
        do {
            TransportProtos.GetHttpPullDevicesResponseMsg response = protoEntityService.getHttpPullDevicesIds(batchIndex, batchSize);
            for (String id : response.getIdsList()) {
                loaded.add(new DeviceId(UUID.fromString(id)));
            }
            next = response.getHasNextPage();
            batchIndex++;
        } while (next);
        allHttpPullDeviceIds.clear();
        allHttpPullDeviceIds.addAll(loaded);
    }

    public boolean isManagedByCurrentTransport(UUID entityId) {
        return balancingService.isManagedByCurrentTransport(entityId);
    }

    private boolean isHttpPullEnabled() {
        return true;
    }

    private void tryEstablishCollector(Device device) {
        if (device == null) {
            return;
        }
        DeviceId deviceId = device.getId();
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getTransportType() != DeviceTransportType.HTTP_PULL) {
            allHttpPullDeviceIds.remove(deviceId);
            return;
        }
        allHttpPullDeviceIds.add(deviceId);
        if (!balancingService.isManagedByCurrentTransport(deviceId.getId())) {
            log.debug("[{}] HTTP pull collector is not managed by current HTTP transport node", deviceId);
            return;
        }
        if (collectorSessions.containsKey(deviceId)) {
            return;
        }
        if (!(profile.getProfileData().getTransportConfiguration() instanceof HttpPullDeviceProfileTransportConfiguration profileCfg)) {
            return;
        }
        if (!establishingCollectors.add(deviceId)) {
            return;
        }
        boolean authSubmitted = false;
        try {
            HttpPullDeviceTransportConfiguration deviceCfg = device.getDeviceData() != null
                    && device.getDeviceData().getTransportConfiguration() instanceof HttpPullDeviceTransportConfiguration h
                    ? h : new HttpPullDeviceTransportConfiguration();
            establishCollectorSession(device, profile, profileCfg, deviceCfg);
            authSubmitted = true;
        } finally {
            if (!authSubmitted) {
                establishingCollectors.remove(deviceId);
            }
        }
    }

    private void establishCollectorSession(Device device, DeviceProfile profile,
                                           HttpPullDeviceProfileTransportConfiguration profileCfg,
                                           HttpPullDeviceTransportConfiguration deviceCfg) {
        DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (credentials.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            log.warn("[{}] HTTP pull collector requires ACCESS_TOKEN credentials", device.getId());
            establishingCollectors.remove(device.getId());
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
            try {
                if (msg == null || !msg.hasDeviceInfo()) {
                    return;
                }
                if (!balancingService.isManagedByCurrentTransport(device.getId().getId())) {
                    return;
                }
                SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, this, UUID.randomUUID());
                ctx.setSessionInfo(sessionInfo);
                registerHttpPullTransportSession(sessionInfo, new HttpPullRpcSessionListener(httpPullRpcService, ctx));
                HttpPullCollectorSessionContext previous = collectorSessions.put(device.getId(), ctx);
                if (previous != null && previous != ctx) {
                    destroyCollector(previous, false);
                }
                if (!isCurrentCollector(ctx)) {
                    destroyCollector(ctx, true);
                    return;
                }
                httpPullTransportService.createQueryingTasks(ctx);
                log.info("Established HTTP pull collector session for {} (inactive until first successful poll)", device.getId());
            } finally {
                establishingCollectors.remove(device.getId());
            }
        });
    }

    private boolean isCurrentCollector(HttpPullCollectorSessionContext ctx) {
        return ctx != null && collectorSessions.get(ctx.getDeviceId()) == ctx;
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
                        establishingCollectors.remove(ctx.getDeviceId());
                        log.warn("[{}] HTTP pull collector auth failed", ctx.getDeviceId(), e);
                    }
                });
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        if (!isHttpPullEnabled()) {
            return;
        }
        refreshCollectorDevice(event.getDevice());
    }

    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        if (!isHttpPullEnabled()) {
            return;
        }
        DeviceId deviceId = event.getDeviceId();
        allHttpPullDeviceIds.remove(deviceId);
        HttpPullCollectorSessionContext collector = collectorSessions.get(deviceId);
        if (collector != null) {
            log.info("Destroying HTTP pull collector session for deleted device {}", deviceId);
            destroyCollector(collector);
        }
    }

    @EventListener(DeviceProfileUpdatedEvent.class)
    public void onDeviceProfileUpdated(DeviceProfileUpdatedEvent event) {
        if (!isHttpPullEnabled()) {
            return;
        }
        DeviceProfile profile = event.getDeviceProfile();
        List<HttpPullCollectorSessionContext> affected = collectorSessions.values().stream()
                .filter(ctx -> ctx.getDeviceProfile().getId().equals(profile.getId()))
                .toList();
        if (affected.isEmpty()) {
            return;
        }
        log.info("Refreshing {} HTTP pull collector session(s) after device profile {} update",
                affected.size(), profile.getId());
        for (HttpPullCollectorSessionContext ctx : new ArrayList<>(affected)) {
            Device device = protoEntityService.getDeviceById(ctx.getDeviceId());
            if (device == null || profile.getTransportType() != DeviceTransportType.HTTP_PULL) {
                destroyCollector(ctx);
            } else {
                refreshCollectorDevice(device);
            }
        }
    }

    private void refreshCollectorDevice(Device device) {
        if (device == null) {
            return;
        }
        DeviceId deviceId = device.getId();
        HttpPullCollectorSessionContext existing = collectorSessions.get(deviceId);
        if (!balancingService.isManagedByCurrentTransport(deviceId.getId())) {
            if (existing != null) {
                destroyCollector(existing);
            }
            return;
        }
        if (existing != null) {
            destroyCollector(existing);
        }
        tryEstablishCollector(device);
    }

    @EventListener(HttpTransportListChangedEvent.class)
    public void onHttpTransportListChanged(HttpTransportListChangedEvent event) {
        log.info("HTTP transport list changed, refreshing pull collectors");
        reconcileCollectors();
    }

    private void registerHttpPullTransportSession(SessionInfoProto sessionInfo, SessionMsgListener listener) {
        transportService.registerAsyncSession(sessionInfo, listener);
    }

    /**
     * 首次成功拉取/出站 RPC 后激活会话：向 Core 上报 OPEN 并订阅 RPC。
     * 避免仅注册传输会话、厂家实际不通时设备仍显示「活跃」。
     */
    public void activateHttpPullDeviceSession(SessionInfoProto sessionInfo, DeviceId collectorDeviceId) {
        if (sessionInfo == null) {
            return;
        }
        DeviceId deviceId = new DeviceId(new UUID(sessionInfo.getDeviceIdMSB(), sessionInfo.getDeviceIdLSB()));
        HttpPullCollectorSessionContext current = collectorDeviceId != null
                ? collectorSessions.get(collectorDeviceId) : collectorSessions.get(deviceId);
        if (current == null || current.getSessionInfo() == null
                || current.getSessionInfo().getSessionIdMSB() != sessionInfo.getSessionIdMSB()
                || current.getSessionInfo().getSessionIdLSB() != sessionInfo.getSessionIdLSB()) {
            return;
        }
        UUID sessionId = new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB());
        if (!activatedTransportSessions.add(sessionId)) {
            return;
        }
        transportService.process(sessionInfo, DefaultTransportService.SESSION_EVENT_MSG_OPEN, null);
        transportService.process(sessionInfo, DefaultTransportService.SUBSCRIBE_TO_RPC_ASYNC_MSG, TransportServiceCallback.EMPTY);
        if (collectorDeviceId != null && collectorDeviceId.equals(deviceId)) {
            TenantId tenantId = new TenantId(new UUID(sessionInfo.getTenantIdMSB(), sessionInfo.getTenantIdLSB()));
            transportService.lifecycleEvent(tenantId, deviceId, ComponentLifecycleEvent.STARTED, true, null);
        }
        log.debug("Activated HTTP pull session for device {}", deviceId);
    }

    private void forgetActivatedTransportSession(SessionInfoProto sessionInfo) {
        if (sessionInfo == null) {
            return;
        }
        activatedTransportSessions.remove(new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB()));
    }

    private void destroyCollector(HttpPullCollectorSessionContext ctx) {
        destroyCollector(ctx, true);
    }

    private void destroyCollector(HttpPullCollectorSessionContext ctx, boolean reportStopped) {
        if (ctx == null) {
            return;
        }
        if (ctx.getSessionInfo() != null) {
            forgetActivatedTransportSession(ctx.getSessionInfo());
            transportService.deregisterSession(ctx.getSessionInfo());
        }
        httpPullTransportService.cancelQueryingTasks(ctx);
        ctx.close();
        collectorSessions.remove(ctx.getDeviceId(), ctx);
        establishingCollectors.remove(ctx.getDeviceId());
        if (reportStopped) {
            transportService.lifecycleEvent(ctx.getTenantId(), ctx.getDeviceId(), ComponentLifecycleEvent.STOPPED, true, null);
        }
    }
}
