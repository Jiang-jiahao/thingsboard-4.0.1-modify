package org.thingsboard.server.transport.http.outbound;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.thingsboard.common.util.AfterStartUp;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.data.id.DeviceId;
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
import org.thingsboard.server.transport.http.HttpTransportBalancingService;
import org.thingsboard.server.transport.http.event.HttpTransportListChangedEvent;
import org.thingsboard.server.transport.http.pull.HttpPullRpcService;
import org.thingsboard.server.transport.http.pull.service.HttpPullProtoEntityService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * DEFAULT（HTTP 被动）设备上 HTTP_OUTBOUND 方法的虚拟会话：无需 long-poll 即可出站。
 */
@Component
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.http.enabled:true}'=='true'")
@Slf4j
@RequiredArgsConstructor
public class HttpOutboundTransportContext extends TransportContext {

    private final HttpPullRpcService httpPullRpcService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final HttpPullProtoEntityService protoEntityService;
    private final HttpTransportBalancingService balancingService;

    private final Map<DeviceId, HttpOutboundSessionContext> outboundSessions = new ConcurrentHashMap<>();
    private final Set<DeviceId> allDefaultDeviceIds = ConcurrentHashMap.newKeySet();
    private final Set<DeviceId> establishing = ConcurrentHashMap.newKeySet();

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void initOutboundSessions() {
        log.info("Initializing HTTP outbound sessions for DEFAULT devices");
        reconcile();
        getScheduler().schedule(this::reconcile, 5, TimeUnit.SECONDS);
        getScheduler().schedule(this::reconcile, 15, TimeUnit.SECONDS);
    }

    private synchronized void reconcile() {
        try {
            reloadDefaultDeviceIds();
            for (DeviceId deviceId : allDefaultDeviceIds) {
                if (!balancingService.isManagedByCurrentTransport(deviceId.getId())) {
                    HttpOutboundSessionContext existing = outboundSessions.get(deviceId);
                    if (existing != null) {
                        destroySession(existing);
                    }
                    continue;
                }
                if (!outboundSessions.containsKey(deviceId) && !establishing.contains(deviceId)) {
                    Device device = protoEntityService.getDeviceById(deviceId);
                    if (device != null) {
                        getExecutor().execute(() -> tryEstablish(device));
                    }
                }
            }
            for (DeviceId id : new ArrayList<>(outboundSessions.keySet())) {
                if (!allDefaultDeviceIds.contains(id) || !balancingService.isManagedByCurrentTransport(id.getId())) {
                    destroySession(outboundSessions.get(id));
                } else {
                    HttpOutboundSessionContext ctx = outboundSessions.get(id);
                    if (ctx != null && !profileHasHttpOutbound(ctx.getDeviceProfile())) {
                        destroySession(ctx);
                    }
                }
            }
            log.info("HTTP outbound reconcile: defaultDevices={}, sessions={}",
                    allDefaultDeviceIds.size(), outboundSessions.size());
        } catch (Exception e) {
            log.warn("Failed to reconcile HTTP outbound sessions", e);
        }
    }

    private void reloadDefaultDeviceIds() {
        Set<DeviceId> loaded = ConcurrentHashMap.newKeySet();
        int page = 0;
        boolean next;
        do {
            TransportProtos.GetHttpPullDevicesResponseMsg response = protoEntityService.getDevicesIdsByTransportType(
                    DeviceTransportType.DEFAULT.name(), page, 512);
            response.getIdsList().stream()
                    .map(id -> new DeviceId(UUID.fromString(id)))
                    .forEach(loaded::add);
            next = response.getHasNextPage();
            page++;
        } while (next);
        allDefaultDeviceIds.clear();
        allDefaultDeviceIds.addAll(loaded);
    }

    private void tryEstablish(Device device) {
        if (device == null || device.getId() == null) {
            return;
        }
        DeviceId deviceId = device.getId();
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getTransportType() != DeviceTransportType.DEFAULT) {
            return;
        }
        if (!profileHasHttpOutbound(profile)) {
            return;
        }
        if (!balancingService.isManagedByCurrentTransport(deviceId.getId())) {
            return;
        }
        if (outboundSessions.containsKey(deviceId) || !establishing.add(deviceId)) {
            return;
        }
        boolean submitted = false;
        try {
            DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(deviceId);
            if (credentials.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
                log.warn("[{}] HTTP outbound session requires ACCESS_TOKEN", deviceId);
                return;
            }
            HttpOutboundSessionContext ctx = HttpOutboundSessionContext.builder()
                    .tenantId(profile.getTenantId())
                    .device(device)
                    .deviceProfile(profile)
                    .token(credentials.getCredentialsId())
                    .transportContext(this)
                    .build();
            transportService.process(DeviceTransportType.DEFAULT,
                    TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(ctx.getToken()).build(),
                    new TransportServiceCallback<>() {
                        @Override
                        public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                            try {
                                if (msg == null || !msg.hasDeviceInfo() || !balancingService.isManagedByCurrentTransport(deviceId.getId())) {
                                    return;
                                }
                                if (!profileHasHttpOutbound(deviceProfileCache.get(device.getDeviceProfileId()))) {
                                    return;
                                }
                                SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, HttpOutboundTransportContext.this, UUID.randomUUID());
                                SessionMsgListener listener = new HttpOutboundRpcSessionListener(httpPullRpcService, ctx);
                                ctx.setSessionInfo(sessionInfo);
                                transportService.registerAsyncSession(sessionInfo, listener);
                                HttpOutboundSessionContext previous = outboundSessions.put(deviceId, ctx);
                                if (previous != null && previous != ctx) {
                                    destroySession(previous);
                                }
                                // 出站会话立即订阅 RPC（无需等待遥测连通）
                                transportService.process(sessionInfo, DefaultTransportService.SESSION_EVENT_MSG_OPEN, null);
                                transportService.process(sessionInfo, DefaultTransportService.SUBSCRIBE_TO_RPC_ASYNC_MSG, TransportServiceCallback.EMPTY);
                                transportService.lifecycleEvent(ctx.getTenantId(), deviceId, ComponentLifecycleEvent.STARTED, true, null);
                                log.info("Established HTTP outbound session for {}", deviceId);
                            } finally {
                                establishing.remove(deviceId);
                            }
                        }

                        @Override
                        public void onError(Throwable e) {
                            establishing.remove(deviceId);
                            log.warn("[{}] HTTP outbound session auth failed", deviceId, e);
                        }
                    });
            submitted = true;
        } finally {
            if (!submitted) {
                establishing.remove(deviceId);
            }
        }
    }

    private void destroySession(HttpOutboundSessionContext ctx) {
        if (ctx == null) {
            return;
        }
        outboundSessions.remove(ctx.getDeviceId(), ctx);
        SessionInfoProto sessionInfo = ctx.getSessionInfo();
        if (sessionInfo != null) {
            transportService.deregisterSession(sessionInfo);
        }
        transportService.lifecycleEvent(ctx.getTenantId(), ctx.getDeviceId(), ComponentLifecycleEvent.STOPPED, true, null);
        log.info("Destroyed HTTP outbound session for {}", ctx.getDeviceId());
    }

    static boolean profileHasHttpOutbound(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getRpcMethods() == null) {
            return false;
        }
        for (DeviceProfileRpcMethod m : profile.getProfileData().getRpcMethods()) {
            if (m != null && m.getBindingType() == DeviceProfileRpcBindingType.HTTP_OUTBOUND) {
                return true;
            }
        }
        return false;
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        Device eventDevice = event.getDevice();
        if (eventDevice == null || eventDevice.getId() == null) {
            return;
        }
        Device device = protoEntityService.getDeviceById(eventDevice.getId());
        if (device == null) {
            destroySession(outboundSessions.get(eventDevice.getId()));
            return;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getTransportType() != DeviceTransportType.DEFAULT || !profileHasHttpOutbound(profile)) {
            destroySession(outboundSessions.get(device.getId()));
            return;
        }
        allDefaultDeviceIds.add(device.getId());
        HttpOutboundSessionContext existing = outboundSessions.get(device.getId());
        if (existing != null) {
            destroySession(existing);
        }
        tryEstablish(device);
    }

    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        allDefaultDeviceIds.remove(event.getDeviceId());
        destroySession(outboundSessions.get(event.getDeviceId()));
    }

    @EventListener(DeviceProfileUpdatedEvent.class)
    public void onDeviceProfileUpdated(DeviceProfileUpdatedEvent event) {
        DeviceProfile profile = event.getDeviceProfile();
        if (profile == null) {
            return;
        }
        List<HttpOutboundSessionContext> affected = outboundSessions.values().stream()
                .filter(ctx -> ctx.getDeviceProfile().getId().equals(profile.getId()))
                .toList();
        for (HttpOutboundSessionContext ctx : new ArrayList<>(affected)) {
            Device device = protoEntityService.getDeviceById(ctx.getDeviceId());
            if (device == null || profile.getTransportType() != DeviceTransportType.DEFAULT || !profileHasHttpOutbound(profile)) {
                destroySession(ctx);
            } else {
                destroySession(ctx);
                tryEstablish(device);
            }
        }
        // 档案新加 HTTP_OUTBOUND 时，补建尚未建会话的 DEFAULT 设备
        if (profile.getTransportType() == DeviceTransportType.DEFAULT && profileHasHttpOutbound(profile)) {
            reconcile();
        }
    }

    @EventListener(HttpTransportListChangedEvent.class)
    public void onHttpTransportListChanged(HttpTransportListChangedEvent event) {
        reconcile();
    }
}
