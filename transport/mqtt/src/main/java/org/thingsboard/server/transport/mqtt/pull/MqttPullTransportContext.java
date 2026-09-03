/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import org.thingsboard.server.transport.mqtt.pull.session.MqttPullNoOpSessionListener;

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
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final MqttPullProtoEntityService protoEntityService;

    private final Map<DeviceId, MqttPullCollectorSessionContext> collectorSessions = new ConcurrentHashMap<>();
    private final Set<UUID> activatedTransportSessions = ConcurrentHashMap.newKeySet();

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
            if (msg == null || !msg.hasDeviceInfo()) {
                return;
            }
            SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, this, UUID.randomUUID());
            registerMqttPullTransportSession(sessionInfo, MqttPullNoOpSessionListener.INSTANCE);
            ctx.setSessionInfo(sessionInfo);
            collectorSessions.put(device.getId(), ctx);
            mqttPullTransportService.connectAndSubscribe(ctx);
            log.info("Established MQTT pull collector session for {}", device.getId());
        });
    }

    public void scheduleReconnect(MqttPullCollectorSessionContext ctx) {
        if (ctx.getReconnectTask() != null) {
            return;
        }
        long delayMs = ctx.getProfileTransportConfiguration().getReconnectIntervalMs() != null
                ? ctx.getProfileTransportConfiguration().getReconnectIntervalMs() : 5000L;
        ScheduledFuture<?> task = getScheduler().schedule(() -> {
            ctx.setReconnectTask(null);
            if (!collectorSessions.containsKey(ctx.getDeviceId())) {
                return;
            }
            mqttPullTransportService.disconnectQuietly(ctx);
            mqttPullTransportService.connectAndSubscribe(ctx);
        }, delayMs, TimeUnit.MILLISECONDS);
        ctx.setReconnectTask(task);
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
            destroyCollector(collector);
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
        MqttPullCollectorSessionContext existing = collectorSessions.get(device.getId());
        if (existing != null) {
            destroyCollector(existing);
        }
        tryEstablishCollector(device);
    }

    private void registerMqttPullTransportSession(SessionInfoProto sessionInfo, org.thingsboard.server.common.transport.SessionMsgListener listener) {
        transportService.registerAsyncSession(sessionInfo, listener);
    }

    public void activateMqttPullDeviceSession(SessionInfoProto sessionInfo, DeviceId collectorDeviceId) {
        if (sessionInfo == null) {
            return;
        }
        UUID sessionId = new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB());
        if (!activatedTransportSessions.add(sessionId)) {
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
     * 外部 Broker 连接成功：上报会话 OPEN，更新 lastConnectTime / active。
     */
    public void onMqttBrokerConnected(MqttPullCollectorSessionContext ctx) {
        if (ctx == null || ctx.getSessionInfo() == null) {
            return;
        }
        ctx.setBrokerLinkActive(true);
        activateMqttPullDeviceSession(ctx.getSessionInfo(), ctx.getDeviceId());
        transportService.recordActivity(ctx.getSessionInfo());
    }

    /**
     * 外部 Broker 连接失败 / 链路断开：立刻写错误事件，并立即标为非活动。
     */
    public void onMqttBrokerFailed(MqttPullCollectorSessionContext ctx, String method, Throwable error) {
        if (ctx == null) {
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
        markMqttPullDeviceInactive(ctx);
    }

    private void markMqttPullDeviceInactive(MqttPullCollectorSessionContext ctx) {
        if (ctx == null) {
            return;
        }
        boolean wasLinked = ctx.isBrokerLinkActive();
        ctx.setBrokerLinkActive(false);
        if (ctx.getSessionInfo() != null) {
            UUID sessionId = new UUID(ctx.getSessionInfo().getSessionIdMSB(), ctx.getSessionInfo().getSessionIdLSB());
            activatedTransportSessions.remove(sessionId);
            if (wasLinked) {
                transportService.closeSessionWithoutReportingActivity(ctx.getSessionInfo());
            }
        }
        transportService.reportDeviceInactivity(ctx.getTenantId(), ctx.getDeviceId());
    }

    private void forgetActivatedTransportSession(SessionInfoProto sessionInfo) {
        if (sessionInfo != null) {
            activatedTransportSessions.remove(new UUID(sessionInfo.getSessionIdMSB(), sessionInfo.getSessionIdLSB()));
        }
    }

    private void destroyCollector(MqttPullCollectorSessionContext ctx) {
        if (ctx == null) {
            return;
        }
        if (ctx.getSessionInfo() != null) {
            forgetActivatedTransportSession(ctx.getSessionInfo());
            transportService.deregisterSession(ctx.getSessionInfo());
        }
        mqttPullTransportService.disconnectQuietly(ctx);
        ctx.close();
        collectorSessions.remove(ctx.getDeviceId());
        transportService.lifecycleEvent(ctx.getTenantId(), ctx.getDeviceId(), ComponentLifecycleEvent.STOPPED, true, null);
    }
}
