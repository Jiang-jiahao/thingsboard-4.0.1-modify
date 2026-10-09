package com.jnks.iot.server.transport.udp.outbound;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.AfterStartUp;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.security.DeviceCredentials;
import com.jnks.iot.server.common.data.security.DeviceCredentialsType;
import com.jnks.iot.server.common.transport.DeviceDeletedEvent;
import com.jnks.iot.server.common.transport.DeviceProfileUpdatedEvent;
import com.jnks.iot.server.common.transport.DeviceUpdatedEvent;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.common.transport.TransportContext;
import com.jnks.iot.server.common.transport.TransportDeviceProfileCache;
import com.jnks.iot.server.common.transport.auth.SessionInfoCreator;
import com.jnks.iot.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import com.jnks.iot.server.common.transport.service.DefaultTransportService;
import com.jnks.iot.server.common.transport.TransportServiceCallback;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.transport.udp.UdpTransportBalancingService;
import com.jnks.iot.server.transport.udp.UdpTransportContext;
import com.jnks.iot.server.transport.udp.UdpTransportService;
import com.jnks.iot.server.transport.udp.service.UdpDownlinkAddressRegistry;
import com.jnks.iot.server.transport.udp.service.UdpProtoTransportEntityService;
import com.jnks.iot.server.transport.udp.util.UdpRpcFrameEncoder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * UDP **服务端模式**下，给"设备还没开口（没有会话）、但已经配好固定下行地址"的设备建一个
 * **出站会话**，让下发的 RPC 有地方可去。
 * <p>
 * 为什么需要：Core 只会把 RPC 投给它**认识会话**的设备（{@code rpcSubscriptions} 非空），
 * 否则消息留在 pending。被动设备从不发包 → 平台永远没有它的会话 → 定时 RPC 永远发不出去。
 * HTTP 侧用同样的办法解决（{@code HttpOutboundTransportContext} 给 DEFAULT 设备建"虚拟会话"，
 * 让平台可以主动出站调用），这里照搬那个模式。
 * <p>
 * 关键区别与约束：
 * <ul>
 *   <li>**不代表设备在线**：Core 判活跃只看 {@code lastActivityTime}（有没有收到数据），
 *       建会话不刷新它。所以"收到回包才算活跃"的语义不变。</li>
 *   <li>**只在设备没有真实会话时建**：设备真开口后（回包落在监听端口 → 建真实会话），
 *       这里的出站会话会被 Core 因并发上限踢掉（{@link UdpOutboundRpcSessionListener} 收到即销毁），
 *       之后的 RPC 走真实会话的正常链路。</li>
 *   <li>**下发必须从档案的监听端口发出**：设备回包才会落回同一个端口、按源 IP / 协议设备 ID 认领。</li>
 * </ul>
 */
@Component
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.udp.enabled:true}'=='true'")
@Slf4j
@RequiredArgsConstructor
public class UdpOutboundTransportContext extends TransportContext {

    private final UdpProtoTransportEntityService protoEntityService;
    private final TransportDeviceProfileCache deviceProfileCache;
    private final UdpTransportBalancingService balancingService;
    private final UdpDownlinkAddressRegistry downlinkAddressRegistry;

    @Autowired
    @Lazy
    private UdpTransportService udpTransportService;

    @Autowired
    @Lazy
    private UdpTransportContext udpTransportContext;

    private final Map<DeviceId, UdpOutboundSessionContext> outboundSessions = new ConcurrentHashMap<>();
    private final Set<DeviceId> candidateDeviceIds = ConcurrentHashMap.newKeySet();
    private final Set<DeviceId> establishing = ConcurrentHashMap.newKeySet();

    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void initOutboundSessions() {
        log.info("Initializing UDP outbound sessions for server-mode devices with a fixed downlink address");
        reconcile();
        // 周期性重建：设备被判空闲回收后，还要能再次给它发 RPC；设备真开口时会话由 Core 踢掉、这里不再建
        getScheduler().scheduleWithFixedDelay(this::reconcile, 30, 30, TimeUnit.SECONDS);
    }

    private synchronized void reconcile() {
        try {
            reloadCandidateDeviceIds();
            for (DeviceId deviceId : candidateDeviceIds) {
                if (!balancingService.isManagedByCurrentTransport(deviceId.getId())) {
                    destroySession(outboundSessions.get(deviceId));
                    continue;
                }
                // 设备已经有真实会话：出站会话则销毁（当设备未上报数据的时候，设备需要下发rpc请求，这个时候需要伪造一个出站会话来订阅rpc）
                if (udpTransportContext != null && udpTransportContext.hasActiveServerSession(deviceId)) {
                    destroySession(outboundSessions.get(deviceId));
                    continue;
                }
                // 如果出站会话不存在，则创建出站会话
                if (!outboundSessions.containsKey(deviceId) && !establishing.contains(deviceId)) {
                    Device device = protoEntityService.getDeviceById(deviceId);
                    if (device != null) {
                        getExecutor().execute(() -> tryEstablish(device));
                    }
                }
            }
            for (DeviceId id : new ArrayList<>(outboundSessions.keySet())) {
                if (!candidateDeviceIds.contains(id) || !balancingService.isManagedByCurrentTransport(id.getId())) {
                    destroySession(outboundSessions.get(id));
                }
            }
            log.debug("UDP outbound reconcile: candidates={}, sessions={}", candidateDeviceIds.size(), outboundSessions.size());
        } catch (Exception e) {
            log.warn("Failed to reconcile UDP outbound sessions", e);
        }
    }

    /**
     * 候选 = 本实例上"UDP + 配了固定下行地址"的设备。
     * 下行地址直接查 {@link UdpDownlinkAddressRegistry}（内存映射，不发 RPC）。
     */
    private void reloadCandidateDeviceIds() {
        Set<DeviceId> loaded = ConcurrentHashMap.newKeySet();
        int page = 0;
        boolean next;
        do {
            TransportProtos.GetUdpDevicesResponseMsg response = protoEntityService.getUdpDevicesIds(page, 512);
            for (String id : response.getIdsList()) {
                DeviceId deviceId = new DeviceId(UUID.fromString(id));
                if (downlinkAddressRegistry.resolve(deviceId) == null) {
                    continue;
                }
                DeviceProfile profile = profileOf(deviceId);
                if (profile != null && isUdpProfile(profile)) {
                    loaded.add(deviceId);
                }
            }
            next = response.getHasNextPage();
            page++;
        } while (next);
        candidateDeviceIds.clear();
        candidateDeviceIds.addAll(loaded);
    }

    private DeviceProfile profileOf(DeviceId deviceId) {
        Device device = protoEntityService.getDeviceById(deviceId);
        return device == null || device.getDeviceProfileId() == null
                ? null
                : deviceProfileCache.get(device.getDeviceProfileId());
    }

    private static boolean isUdpProfile(DeviceProfile profile) {
        return profile != null && profile.getTransportType() == DeviceTransportType.UDP
                && profile.getProfileData() != null
                && profile.getProfileData().getTransportConfiguration()
                        instanceof UdpDeviceProfileTransportConfiguration;
    }

    private void tryEstablish(Device device) {
        if (device == null || device.getId() == null) {
            return;
        }
        DeviceId deviceId = device.getId();
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (!isUdpProfile(profile)) {
            return;
        }
        if (!balancingService.isManagedByCurrentTransport(deviceId.getId())) {
            return;
        }
        if (udpTransportContext != null && udpTransportContext.hasActiveServerSession(deviceId)) {
            return;
        }
        if (outboundSessions.containsKey(deviceId) || !establishing.add(deviceId)) {
            return;
        }
        boolean submitted = false;
        try {
            DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(deviceId);
            if (credentials == null || credentials.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
                log.warn("[{}] UDP outbound session requires ACCESS_TOKEN credentials", deviceId);
                return;
            }
            UdpOutboundSessionContext ctx = UdpOutboundSessionContext.builder()
                    .device(device)
                    .deviceProfile(profile)
                    .token(credentials.getCredentialsId())
                    .transportContext(this)
                    .build();
            transportService.process(DeviceTransportType.UDP,
                    TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(ctx.getToken()).build(),
                    new TransportServiceCallback<>() {
                        @Override
                        public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                            try {
                                if (msg == null || !msg.hasDeviceInfo()
                                        || !balancingService.isManagedByCurrentTransport(deviceId.getId())) {
                                    return;
                                }
                                if (udpTransportContext != null && udpTransportContext.hasActiveServerSession(deviceId)) {
                                    return;
                                }
                                if (downlinkAddressRegistry.resolve(deviceId) == null) {
                                    return;
                                }
                                TransportProtos.SessionInfoProto sessionInfo = SessionInfoCreator.create(
                                        msg, UdpOutboundTransportContext.this, UUID.randomUUID());
                                SessionMsgListener listener = new UdpOutboundRpcSessionListener(ctx);
                                ctx.setSessionInfo(sessionInfo);
                                transportService.registerAsyncSession(sessionInfo, listener);
                                UdpOutboundSessionContext previous = outboundSessions.put(deviceId, ctx);
                                if (previous != null && previous != ctx) {
                                    destroySession(previous);
                                }
                                // 只订阅 RPC，**不发 SESSION_EVENT_MSG_OPEN** ——
                                // Core 收到会话打开会顺手记一次活动（DeviceActorMessageProcessor 里
                                // `onDeviceActivity(now)`），那会让"一个包都没发"的设备显示活跃，
                                // 与"收到数据才算活跃"矛盾。RPC 下发靠的是下面这条订阅（rpcSubscriptions）。
                                transportService.process(sessionInfo, DefaultTransportService.SUBSCRIBE_TO_RPC_ASYNC_MSG,
                                        TransportServiceCallback.EMPTY);
                                // 刻意**不发**生命周期事件：它记录的是"设备连上了"，而这里设备一个包都没发过 ——
                                // 发了就是误导（而且配了固定下行地址 + 真实会话落在别的实例时，
                                // 这条 STARTED 永远等不到 STOPPED，见下面 destroySession 的说明）。
                                log.info("[{}] Established UDP outbound session (device has no session yet)", deviceId);
                            } finally {
                                establishing.remove(deviceId);
                            }
                        }

                        @Override
                        public void onError(Throwable e) {
                            establishing.remove(deviceId);
                            log.warn("[{}] UDP outbound session auth failed", deviceId, e);
                        }
                    });
            submitted = true;
        } catch (Exception e) {
            log.warn("[{}] Failed to establish UDP outbound session", deviceId, e);
        } finally {
            if (!submitted) {
                establishing.remove(deviceId);
            }
        }
    }

    /**
     * 设备还没有会话时的下发：**从档案的监听端口**把编好的帧发到设备配置的固定下行地址。
     * <p>
     * 用它发而不是用某个会话发，是为了让设备的回包落回**同一个监听端口**，从而按正常的入站链路
     * 建会话、认领设备、刷新活跃。不回包就什么都没有 —— 设备依旧是非活跃。
     */
    public void sendRpcWithoutSession(UdpOutboundSessionContext ctx, TransportProtos.ToDeviceRpcRequestMsg rpcRequest) {
        DeviceId deviceId = ctx.getDeviceId();
        InetSocketAddress target = downlinkAddressRegistry.resolve(deviceId);
        if (target == null) {
            log.warn("[{}] No UDP downlink address configured; dropping RPC [{}]",
                    deviceId, rpcRequest.getMethodName());
            return;
        }
        Integer listenPort = udpTransportContext == null ? null : udpTransportContext.resolveProfileListenPort(ctx.getDeviceProfile());
        Channel channel = listenPort == null ? null : udpTransportService.getListenChannel(listenPort);
        if (channel == null || !channel.isActive()) {
            log.warn("[{}] UDP listen port {} is not bound on this instance; dropping RPC [{}]",
                    deviceId, listenPort, rpcRequest.getMethodName());
            return;
        }
        ByteBuf frame = UdpRpcFrameEncoder.encode(ctx.getDeviceProfile(), rpcRequest);
        log.debug("[{}] Sending RPC [{}] without session: listen port {} -> {}", deviceId,
                rpcRequest.getMethodName(), listenPort, target);
        channel.writeAndFlush(new DatagramPacket(frame, target));
    }

    public void destroySession(UdpOutboundSessionContext ctx) {
        if (ctx == null) {
            return;
        }
        outboundSessions.remove(ctx.getDeviceId(), ctx);
        TransportProtos.SessionInfoProto sessionInfo = ctx.getSessionInfo();
        if (sessionInfo != null) {
            // 必须显式退订：{@code deregisterSession} 只清**传输层本地**的会话表，不通知 Core。
            // 不退订的话 Core 侧的 rpcSubscriptions 会一直留着这条已死的会话
            // （每次重建出站会话都是新 sessionId）→ 陈旧订阅越积越多，下发时还会照投一份（再被传输层丢弃）。
            transportService.process(sessionInfo,
                    TransportProtos.SubscribeToRPCMsg.newBuilder().setUnsubscribe(true).build(),
                    TransportServiceCallback.EMPTY);
            transportService.process(sessionInfo,
                    TransportProtos.SubscribeToAttributeUpdatesMsg.newBuilder().setUnsubscribe(true).build(),
                    TransportServiceCallback.EMPTY);
            transportService.deregisterSession(sessionInfo);
        }
        // 与建立时对称：虚拟会话不写生命周期事件（否则会留下一条配不上对的 STOPPED）。
        log.info("[{}] Destroyed UDP outbound session", ctx.getDeviceId());
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdated(DeviceUpdatedEvent event) {
        if (event.getDevice() == null || event.getDevice().getId() == null) {
            return;
        }
        DeviceId deviceId = event.getDevice().getId();
        Device device = protoEntityService.getDeviceById(deviceId);
        if (device == null || downlinkAddressRegistry.resolve(deviceId) == null
                || !isUdpProfile(deviceProfileCache.get(device.getDeviceProfileId()))) {
            destroySession(outboundSessions.get(deviceId));
            return;
        }
        destroySession(outboundSessions.get(deviceId));
        getExecutor().execute(() -> tryEstablish(device));
    }

    @EventListener(DeviceDeletedEvent.class)
    public void onDeviceDeleted(DeviceDeletedEvent event) {
        destroySession(outboundSessions.get(event.getDeviceId()));
    }

    @EventListener(DeviceProfileUpdatedEvent.class)
    public void onDeviceProfileUpdated(DeviceProfileUpdatedEvent event) {
        DeviceProfile profile = event.getDeviceProfile();
        if (profile == null || profile.getId() == null) {
            return;
        }
        List<UdpOutboundSessionContext> affected = outboundSessions.values().stream()
                .filter(ctx -> ctx.getDeviceProfile() != null && profile.getId().equals(ctx.getDeviceProfile().getId()))
                .toList();
        for (UdpOutboundSessionContext ctx : affected) {
            destroySession(ctx);
        }
        getScheduler().schedule(this::reconcile, 3, TimeUnit.SECONDS);
    }

    @EventListener(com.jnks.iot.server.transport.udp.event.UdpTransportListChangedEvent.class)
    public void onUdpTransportListChanged(com.jnks.iot.server.transport.udp.event.UdpTransportListChangedEvent event) {
        getScheduler().schedule(this::reconcile, 3, TimeUnit.SECONDS);
    }
}
