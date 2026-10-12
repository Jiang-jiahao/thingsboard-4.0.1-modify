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
 * UDP **服务端模式**下，给"档案里显式配好固定下行地址"的设备建一个**出站会话**，
 * 它是这类**唯一**的 RPC 下发通道。
 * <p>
 * 为什么需要：UDP 的真实会话不是下发通道 —— 它只做"免重复鉴权 + 刷新活跃"，
 * 不订阅 RPC（见 {@code UdpTransportContext#completeSessionRegistration}）。
 * 设备和 HTTP 一样不保持连接，靠会话残留当通道会造出"显示活跃却发不到"的窗口，
 * 所以"能不能下发"改成**显式配置**：档案里填了 {@code udpDownlinkHost/Port} 才建出站会话。
 * HTTP 侧的 pull / outbound 是同一套思路。
 * <p>
 * 关键区别与约束：
 * <ul>
 *   <li>**不代表设备在线**：Core 判活跃只看 {@code lastActivityTime}（有没有收到数据），
 *       建会话不刷新它。所以"收到回包才算活跃"的语义不变。</li>
 *   <li>**与真实会话并存**（2026-10-12 起）：设备开口后真实会话照旧建，但出站会话**不销毁** ——
 *       真实会话不再占 RPC 订阅，出站会话是唯一的下发通道，拆了就没通道了。</li>
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
                // 设备已经有真实会话：出站会话照样保留 —— 真实会话在 UDP 里只做鉴权缓存与刷活跃，
                // **不是**下发通道（见 UdpTransportContext#completeSessionRegistration），
                // 所以这里不能因为设备开口就把它拆掉，否则设备一上报就没有可下发的通道了。
                // 如果出站会话不存在，则创建出站会话
                if (!outboundSessions.containsKey(deviceId) && !establishing.contains(deviceId)) {
                    Device device = protoEntityService.getDeviceById(deviceId);
                    if (device != null) {
                        getExecutor().execute(() -> tryEstablish(device, false));
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

    /**
     * @param force 设备/档案刚变更时传 true：**先建后拆**（新建的会话在原位把旧的换掉，见下方 auth 回调里的
     *              {@code put + destroy(previous)}），避免"先拆后建"之间那段没有通道的窗口 —— 那时 Core 还留着
     *              订阅、RPC 照回 200，但传输层已经没有会话可发（实测 12 发丢 1~4 条，静默丢）。
     */
    private void tryEstablish(Device device, boolean force) {
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
        if ((!force && outboundSessions.containsKey(deviceId)) || !establishing.add(deviceId)) {
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
                                if (downlinkAddressRegistry.resolve(deviceId) == null) {
                                    return;
                                }
                                TransportProtos.SessionInfoProto sessionInfo = SessionInfoCreator.create(
                                        msg, UdpOutboundTransportContext.this, UUID.randomUUID());
                                SessionMsgListener listener = new UdpOutboundRpcSessionListener(ctx);
                                ctx.setSessionInfo(sessionInfo);
                                // 它按设计**不报活动**（不伪造设备活跃）→ 必须豁免"不活动过期"，
                                // 否则每 transport.sessions.inactivity_timeout 就被收一次、打断唯一的下发通道。
                                transportService.registerAsyncSession(sessionInfo, listener).setExpiryExempt(true);
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
                                log.info("[{}] Established UDP outbound session ({})", deviceId,
                                        previous == null ? "new" : "rebuilt, previous replaced");
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
        // 排查"下发成功但设备收不到"的关键一行：平台确实把帧写进 socket 了、发去了哪里。
        // 设备侧没收到时拿它对齐——目的地址是档案里配的固定下行地址，路上被 NAT/防火墙丢掉的话
        // 平台侧只有这一行可对照（UDP 没有回执）。查完记得把级别调回去。
        log.info("[{}] UDP downlink RPC [{}] requestId={} oneway={} via listen port {} -> {}", deviceId,
                rpcRequest.getMethodName(), rpcRequest.getRequestId(), rpcRequest.getOneway(),
                listenPort, target);
        channel.writeAndFlush(new DatagramPacket(frame, target)).addListener(f -> {
            if (!f.isSuccess()) {
                log.warn("[{}] UDP downlink RPC [{}] requestId={} write failed -> {}: {}", deviceId,
                        rpcRequest.getMethodName(), rpcRequest.getRequestId(), target, f.cause());
            }
        });
    }

    /**
     * 立刻重建该设备的出站会话（不等 30 秒一轮的 reconcile）。
     * <p>
     * 用于"通道刚丢"的场景 —— 目前是会话因不活动过期（虚拟会话从不报活动，所以必然发生）：
     * 不立刻重建的话，那段窗口里的 RPC 会 NO_ACTIVE_CONNECTION、已进队列的会被静默丢弃。
     */
    public void reestablishNow(Device device) {
        if (device == null) {
            return;
        }
        getExecutor().execute(() -> tryEstablish(device, true));
    }

    public void destroySession(UdpOutboundSessionContext ctx) {        if (ctx == null) {
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
        // 仍符合条件：**先建后拆** —— 不能先 destroy，那几秒里 Core 还留着订阅、RPC 会回 200 却发不出去
        getExecutor().execute(() -> tryEstablish(device, true));
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
            Device device = protoEntityService.getDeviceById(ctx.getDeviceId());
            if (device != null) {
                // **先建后拆**：原先是"destroy + 3 秒后 reconcile"，留下最长 3 秒无通道的窗口，
                // 那期间 RPC 照回 200 却发不出去（实测 12 发丢 4 条）。
                getExecutor().execute(() -> tryEstablish(device, true));
            }
        }
        // 兜底：新建失败的（读不到设备等）下一轮 reconcile 会补上
        getScheduler().schedule(this::reconcile, 3, TimeUnit.SECONDS);
    }

    @EventListener(com.jnks.iot.server.transport.udp.event.UdpTransportListChangedEvent.class)
    public void onUdpTransportListChanged(com.jnks.iot.server.transport.udp.event.UdpTransportListChangedEvent event) {
        getScheduler().schedule(this::reconcile, 3, TimeUnit.SECONDS);
    }
}
