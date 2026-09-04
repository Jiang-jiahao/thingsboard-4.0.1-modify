/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.device.data.MqttPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.MqttPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;
import org.thingsboard.server.common.transport.DeviceDeletedEvent;
import org.thingsboard.server.common.transport.DeviceProfileUpdatedEvent;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.SessionMsgListener;
import org.thingsboard.server.common.transport.TransportContext;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.auth.SessionInfoCreator;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.common.transport.service.DefaultTransportService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.transport.mqtt.pull.service.MqttPullProtoEntityService;
import org.thingsboard.server.transport.mqtt.pull.session.MqttPullCollectorSessionContext;
import org.thingsboard.server.transport.mqtt.pull.session.MqttPullRpcSessionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnExpression("'${service.type:null}'=='tb-transport' || ('${service.type:null}'=='monolith' && '${transport.api_enabled:true}'=='true' && '${transport.mqtt.enabled}'=='true')")
@Slf4j
@RequiredArgsConstructor
public class MqttPullTransportContext extends TransportContext {

    @Getter
    private final MqttPullTransportService mqttPullTransportService;
    private final MqttPullRpcService mqttPullRpcService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final MqttPullProtoEntityService protoEntityService;

    private final Map<DeviceId, MqttPullCollectorSessionContext> collectorSessions = new ConcurrentHashMap<>();
    private final Set<UUID> activatedTransportSessions = ConcurrentHashMap.newKeySet();

    @Value("${transport.sessions.inactivity_timeout:300000}")
    private long sessionInactivityTimeout;

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void fetchCollectorsAndEstablishSessions() {
        log.info("Initializing MQTT pull collector sessions");
        int batchIndex = 0;
        int batchSize = 512;
        boolean next;
        do {
            TransportProtos.GetMqttPullDevicesResponseMsg response = protoEntityService.getMqttPullDevicesIds(batchIndex, batchSize);
            response.getIdsList().stream()
                    .map(id -> new DeviceId(UUID.fromString(id)))
                    .map(protoEntityService::getDeviceById)
                    .forEach(device -> getExecutor().execute(() -> tryEstablishCollector(device)));
            next = response.getHasNextPage();
            batchIndex++;
        } while (next);
    }

    private void tryEstablishCollector(Device device) {
        if (device == null) {
            return;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getTransportType() != DeviceTransportType.MQTT_PULL) {
            return;
        }
        if (!(profile.getProfileData().getTransportConfiguration() instanceof MqttPullDeviceProfileTransportConfiguration profileCfg)) {
            return;
        }
        MqttPullDeviceTransportConfiguration deviceCfg = device.getDeviceData() != null
                && device.getDeviceData().getTransportConfiguration() instanceof MqttPullDeviceTransportConfiguration m
                ? m : new MqttPullDeviceTransportConfiguration();
        establishCollectorSession(device, profile, profileCfg, deviceCfg);
    }

    private void establishCollectorSession(Device device, DeviceProfile profile,
                                           MqttPullDeviceProfileTransportConfiguration profileCfg,
                                           MqttPullDeviceTransportConfiguration deviceCfg) {
        DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (credentials.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            log.warn("[{}] MQTT pull collector requires ACCESS_TOKEN credentials", device.getId());
            return;
        }
        MqttPullCollectorSessionContext ctx = MqttPullCollectorSessionContext.builder()
                .tenantId(profile.getTenantId())
                .device(device)
                .deviceProfile(profile)
                .token(credentials.getCredentialsId())
                .profileTransportConfiguration(profileCfg)
                .deviceTransportConfiguration(deviceCfg)
                .transportContext(this)
                .build();
        registerCollectorAuth(ctx, msg -> {
            if (msg == null || !msg.hasDeviceInfo() || ctx.isDestroyed()) {
                return;
            }
            SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, this, UUID.randomUUID());
            SessionMsgListener listener = new MqttPullRpcSessionListener(mqttPullRpcService, ctx);
            ctx.setRpcSessionListener(listener);
            ctx.setSessionInfo(sessionInfo);
            registerMqttPullTransportSession(sessionInfo, listener);
            MqttPullCollectorSessionContext previous = collectorSessions.put(device.getId(), ctx);
            if (previous != null && previous != ctx) {
                destroyCollector(previous, false);
            }
            if (!isCurrentCollector(ctx)) {
                return;
            }
            mqttPullTransportService.connectAndSubscribe(ctx);
            log.info("Established MQTT pull collector session for {}", device.getId());
        });
    }

    public void scheduleReconnect(MqttPullCollectorSessionContext ctx) {
        if (ctx == null || !isCurrentCollector(ctx) || ctx.getReconnectTask() != null) {
            return;
        }
        long delayMs = ctx.getProfileTransportConfiguration().getReconnectIntervalMs() != null
                ? ctx.getProfileTransportConfiguration().getReconnectIntervalMs() : 5000L;
        ScheduledFuture<?> task = getScheduler().schedule(() -> {
            ctx.setReconnectTask(null);
            if (!isCurrentCollector(ctx)) {
                return;
            }
            mqttPullTransportService.disconnectQuietly(ctx);
            mqttPullTransportService.connectAndSubscribe(ctx);
        }, delayMs, TimeUnit.MILLISECONDS);
        ctx.setReconnectTask(task);
    }

    public boolean isCurrentCollector(MqttPullCollectorSessionContext ctx) {
        return ctx != null && !ctx.isDestroyed() && collectorSessions.get(ctx.getDeviceId()) == ctx;
    }

    private void registerCollectorAuth(MqttPullCollectorSessionContext ctx,
                                       java.util.function.Consumer<ValidateDeviceCredentialsResponse> onSuccess) {
        transportService.process(DeviceTransportType.MQTT_PULL,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(ctx.getToken()).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        onSuccess.accept(msg);
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] MQTT pull collector auth failed", ctx.getDeviceId(), e);
                        transportService.errorEvent(ctx.getTenantId(), ctx.getDeviceId(), "mqttPullAuth", e);
                        transportService.reportDeviceInactivity(ctx.getTenantId(), ctx.getDeviceId());
                    }
                });
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        refreshCollectorDevice(event.getDevice());
    }

    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        DeviceId deviceId = event.getDeviceId();
        MqttPullCollectorSessionContext collector = collectorSessions.get(deviceId);
        if (collector != null) {
            destroyCollector(collector, true);
        }
    }

    @EventListener(DeviceProfileUpdatedEvent.class)
    public void onDeviceProfileUpdated(DeviceProfileUpdatedEvent event) {
        DeviceProfile profile = event.getDeviceProfile();
        List<MqttPullCollectorSessionContext> affected = collectorSessions.values().stream()
                .filter(ctx -> ctx.getDeviceProfile().getId().equals(profile.getId()))
                .toList();
        for (MqttPullCollectorSessionContext ctx : new ArrayList<>(affected)) {
            Device device = protoEntityService.getDeviceById(ctx.getDeviceId());
            if (device == null || profile.getTransportType() != DeviceTransportType.MQTT_PULL) {
                destroyCollector(ctx, true);
            } else {
                refreshCollectorDevice(device);
            }
        }
    }

    private void refreshCollectorDevice(Device device) {
        if (device == null) {
            return;
        }
        MqttPullCollectorSessionContext existing = collectorSessions.get(device.getId());
        if (existing != null) {
            destroyCollector(existing, false);
        }
        tryEstablishCollector(device);
    }

    private void registerMqttPullTransportSession(SessionInfoProto sessionInfo, SessionMsgListener listener) {
        transportService.registerAsyncSession(sessionInfo, listener);
    }

    public void activateMqttPullDeviceSession(SessionInfoProto sessionInfo, DeviceId collectorDeviceId) {
        if (sessionInfo == null) {
            return;
        }
        MqttPullCollectorSessionContext ctx = collectorDeviceId != null ? collectorSessions.get(collectorDeviceId) : null;
        if (ctx != null && sessionInfo.equals(ctx.getSessionInfo())) {
            activateMqttPullDeviceSession(ctx, false);
            return;
        }
        activateMqttPullDeviceSession(sessionInfo, collectorDeviceId, false, null);
    }

    public void activateMqttPullDeviceSession(MqttPullCollectorSessionContext ctx, boolean force) {
        if (ctx == null || ctx.getSessionInfo() == null) {
            return;
        }
        activateMqttPullDeviceSession(ctx.getSessionInfo(), ctx.getDeviceId(), force, ctx);
    }

    private void activateMqttPullDeviceSession(SessionInfoProto sessionInfo, DeviceId collectorDeviceId,
                                               boolean force, MqttPullCollectorSessionContext ctx) {
        if (sessionInfo == null) {
            return;
        }
        if (ctx != null && ctx.getRpcSessionListener() != null) {
            registerMqttPullTransportSession(sessionInfo, ctx.getRpcSessionListener());
        }
        UUID sessionId = new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB());
        if (force) {
            activatedTransportSessions.add(sessionId);
        } else if (!activatedTransportSessions.add(sessionId)) {
            return;
        }
        transportService.process(sessionInfo, DefaultTransportService.SESSION_EVENT_MSG_OPEN, null);
        transportService.process(sessionInfo, DefaultTransportService.SUBSCRIBE_TO_RPC_ASYNC_MSG, TransportServiceCallback.EMPTY);
        DeviceId deviceId = new DeviceId(new UUID(sessionInfo.getDeviceIdMSB(), sessionInfo.getDeviceIdLSB()));
        if (collectorDeviceId != null && collectorDeviceId.equals(deviceId)) {
            TenantId tenantId = new TenantId(new UUID(sessionInfo.getTenantIdMSB(), sessionInfo.getTenantIdLSB()));
            transportService.lifecycleEvent(tenantId, deviceId, ComponentLifecycleEvent.STARTED, true, null);
        }
    }

    /**
     * Core / 本地会话超时关闭后：若 Broker 仍连着，用新 sessionId 重建 RPC 会话。
     * 同一 sessionId 再次 OPEN 会被 DeviceActor 当成重复事件丢掉，第二次 10 分钟超时后 RPC 就会失效。
     */
    public void onTransportSessionClosed(MqttPullCollectorSessionContext ctx, UUID sessionId) {
        if (!isCurrentCollector(ctx) || ctx.getSessionInfo() == null || sessionId == null) {
            return;
        }
        UUID currentId = new UUID(ctx.getSessionInfo().getSessionIdMSB(), ctx.getSessionInfo().getSessionIdLSB());
        if (!sessionId.equals(currentId)) {
            return;
        }
        forgetActivatedTransportSession(ctx.getSessionInfo());
        if (!ctx.isBrokerLinkActive()) {
            return;
        }
        getExecutor().execute(() -> {
            if (!isCurrentCollector(ctx) || !ctx.isBrokerLinkActive()) {
                return;
            }
            log.info("[{}] MQTT pull transport session closed while broker is connected, renewing RPC session", ctx.getDeviceId());
            renewMqttPullTransportSession(ctx);
        });
    }

    /**
     * 换新 sessionId 后重新注册、OPEN 并订阅 RPC，避免 DeviceActor 丢弃重复 OPEN。
     */
    private void renewMqttPullTransportSession(MqttPullCollectorSessionContext ctx) {
        SessionInfoProto previous = ctx.getSessionInfo();
        if (previous == null || ctx.getRpcSessionListener() == null) {
            return;
        }
        forgetActivatedTransportSession(previous);
        transportService.closeSessionWithoutReportingActivity(previous);
        transportService.deregisterSession(previous);
        UUID newId = UUID.randomUUID();
        SessionInfoProto next = previous.toBuilder()
                .setSessionIdMSB(newId.getMostSignificantBits())
                .setSessionIdLSB(newId.getLeastSignificantBits())
                .build();
        ctx.setSessionInfo(next);
        ctx.resetRpcState();
        registerMqttPullTransportSession(next, ctx.getRpcSessionListener());
        activateMqttPullDeviceSession(ctx, true);
        transportService.recordActivity(next);
        startSessionHeartbeat(ctx);
        log.info("[{}] MQTT pull RPC session renewed {} -> {}", ctx.getDeviceId(),
                new UUID(previous.getSessionIdMSB(), previous.getSessionIdLSB()), newId);
    }

    /**
     * 外部 Broker 连接成功：重新注册传输会话并订阅 RPC，更新 lastConnectTime / active。
     */
    public void onMqttBrokerConnected(MqttPullCollectorSessionContext ctx) {
        if (!isCurrentCollector(ctx) || ctx.getSessionInfo() == null || ctx.getRpcSessionListener() == null) {
            mqttPullTransportService.disconnectQuietly(ctx);
            return;
        }
        ctx.setBrokerLinkActive(true);
        ctx.resetRpcState();
        activateMqttPullDeviceSession(ctx, true);
        transportService.recordActivity(ctx.getSessionInfo());
        startSessionHeartbeat(ctx);
    }

    /**
     * 外部 Broker 连接失败 / 链路断开：立刻写错误事件，并立即标为非活动。
     */
    public void onMqttBrokerFailed(MqttPullCollectorSessionContext ctx, String method, Throwable error) {
        if (!isCurrentCollector(ctx)) {
            return;
        }
        if (error != null) {
            transportService.errorEvent(ctx.getTenantId(), ctx.getDeviceId(), method, error);
        }
        markMqttPullDeviceInactive(ctx);
    }

    /**
     * 外部 Broker 断开：关闭 Core 会话并立即标为非活动，不等待不活动超时。
     */
    public void onMqttBrokerDisconnected(MqttPullCollectorSessionContext ctx) {
        if (!isCurrentCollector(ctx)) {
            return;
        }
        markMqttPullDeviceInactive(ctx);
    }

    private void startSessionHeartbeat(MqttPullCollectorSessionContext ctx) {
        ctx.cancelActivityHeartbeat();
        long periodMs = Math.max(5_000L, sessionInactivityTimeout / 3);
        ctx.setActivityHeartbeatTask(getScheduler().scheduleWithFixedDelay(() -> {
            try {
                if (!isCurrentCollector(ctx) || !ctx.isBrokerLinkActive() || ctx.getSessionInfo() == null) {
                    return;
                }
                if (ctx.getRpcSessionListener() != null) {
                    registerMqttPullTransportSession(ctx.getSessionInfo(), ctx.getRpcSessionListener());
                }
                transportService.recordActivity(ctx.getSessionInfo());
            } catch (Exception e) {
                log.warn("[{}] MQTT pull session heartbeat failed", ctx.getDeviceId(), e);
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS));
    }

    private void markMqttPullDeviceInactive(MqttPullCollectorSessionContext ctx) {
        if (ctx == null) {
            return;
        }
        boolean wasLinked = ctx.isBrokerLinkActive();
        ctx.setBrokerLinkActive(false);
        ctx.cancelActivityHeartbeat();
        mqttPullRpcService.failPendingRpcs(ctx, "MQTT pull client is not connected");
        ctx.resetRpcState();
        if (ctx.getSessionInfo() != null) {
            UUID sessionId = new UUID(ctx.getSessionInfo().getSessionIdMSB(), ctx.getSessionInfo().getSessionIdLSB());
            activatedTransportSessions.remove(sessionId);
            if (wasLinked) {
                transportService.closeSessionWithoutReportingActivity(ctx.getSessionInfo());
            }
            transportService.deregisterSession(ctx.getSessionInfo());
        }
        transportService.reportDeviceInactivity(ctx.getTenantId(), ctx.getDeviceId());
        if (wasLinked) {
            transportService.lifecycleEvent(ctx.getTenantId(), ctx.getDeviceId(), ComponentLifecycleEvent.STOPPED, true, null);
        }
    }

    private void forgetActivatedTransportSession(SessionInfoProto sessionInfo) {
        if (sessionInfo != null) {
            activatedTransportSessions.remove(new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB()));
        }
    }

    private void destroyCollector(MqttPullCollectorSessionContext ctx, boolean reportInactive) {
        if (ctx == null) {
            return;
        }
        ctx.markDestroyed();
        collectorSessions.remove(ctx.getDeviceId(), ctx);
        ctx.cancelActivityHeartbeat();
        mqttPullRpcService.failPendingRpcs(ctx, "MQTT pull collector destroyed");
        if (ctx.getSessionInfo() != null) {
            forgetActivatedTransportSession(ctx.getSessionInfo());
            transportService.closeSessionWithoutReportingActivity(ctx.getSessionInfo());
            transportService.deregisterSession(ctx.getSessionInfo());
        }
        ctx.setBrokerLinkActive(false);
        if (reportInactive) {
            transportService.reportDeviceInactivity(ctx.getTenantId(), ctx.getDeviceId());
        }
        mqttPullTransportService.disconnectQuietly(ctx);
        ctx.close();
        transportService.lifecycleEvent(ctx.getTenantId(), ctx.getDeviceId(), ComponentLifecycleEvent.STOPPED, true, null);
    }
}
