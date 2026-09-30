package com.jnks.iot.server.transport.udp;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.common.scheduler.SchedulerComponent;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.transport.udp.service.UdpDeferredAuthCatalog;
import com.jnks.iot.server.transport.udp.service.UdpDownlinkAddressRegistry;
import com.jnks.iot.server.transport.udp.service.UdpListenPortRegistry;
import com.jnks.iot.server.transport.udp.service.UdpProtocolDeviceIdRegistry;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import com.jnks.iot.server.common.data.device.data.DeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.data.UdpDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.UdpWireAuthenticationMode;
import com.jnks.iot.server.transport.udp.service.UdpProtoTransportEntityService;
import com.jnks.iot.server.transport.udp.service.UdpSourceBindingService;
import com.jnks.iot.server.common.data.device.profile.UdpTransportFramingMode;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;
import com.jnks.iot.server.common.data.security.DeviceCredentials;
import com.jnks.iot.server.common.data.security.DeviceCredentialsType;
import com.jnks.iot.server.common.transport.DeviceProfileUpdatedEvent;
import com.jnks.iot.server.common.transport.DeviceUpdatedEvent;
import com.jnks.iot.server.common.transport.TransportDeviceProfileCache;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.common.transport.TransportServiceCallback;
import com.jnks.iot.server.common.transport.auth.SessionInfoCreator;
import com.jnks.iot.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import com.jnks.iot.server.common.transport.service.DefaultTransportService;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.common.util.AfterStartUp;
import com.jnks.iot.server.transport.udp.session.UdpDeviceSession;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.udp.enabled:true}'=='true'")
@Component
@Slf4j
public class UdpTransportContext extends com.jnks.iot.server.common.transport.TransportContext {
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final UdpProtoTransportEntityService protoEntityService;
    private final UdpTransportBalancingService balancingService;
    private final UdpSourceBindingService udpSourceBindingService;
    private final UdpProtocolDeviceIdRegistry udpProtocolDeviceIdRegistry;
    private final UdpDeferredAuthCatalog udpDeferredAuthCatalog;
    @Getter
    private final UdpMessageProcessor udpMessageProcessor;

    private final UdpTransportService udpTransportService;

    private final Map<DeviceId, UdpDeviceSession> serverSessions = new ConcurrentHashMap<>();
    private final Collection<DeviceId> allUdpDeviceIds = new ConcurrentLinkedDeque<>();

    /**
     * 所有入站（SERVER）会话：含尚未写入 {@link #serverSessions} 的鉴权中连接。
     * 用于在专用监听端口解绑或设备/档案变更时主动断开；仅关闭 Netty 的 ServerChannel 不会自动关闭已接受的子 TCP 连接。
     */
    /**
     * 入站会话（含鉴权完成前），键是 sessionId。
     * <p>
     * **不能**用 Set 存 {@link UdpDeviceSession}：它继承的 {@code DeviceAwareSessionContext} 带 Lombok @Data，
     * equals/hashCode 覆盖全部字段，而 {@code lastUplinkMs} 每个数据报都在变 —— 作为哈希集合的键时
     * add 会重复插入同一对象、remove 又找不回来（集合只增不减，读空闲扫描每 10 秒重复关同一批会话）。
     * sessionId 是构造时生成的 UUID，不可变，适合做键。
     */
    private final ConcurrentHashMap<UUID, UdpDeviceSession> inboundSessions = new ConcurrentHashMap<>();

    private record UdpPeerKey(int localPort, String host, int port) {
        static UdpPeerKey of(int localPort, InetSocketAddress remote) {
            return new UdpPeerKey(localPort, remote.getAddress().getHostAddress(), remote.getPort());
        }
    }

    private final ConcurrentHashMap<UdpPeerKey, UdpDeviceSession> inboundSessionByPeer = new ConcurrentHashMap<>();

    @Autowired
    private SchedulerComponent scheduler;

    @Autowired
    private UdpDownlinkAddressRegistry udpDownlinkAddressRegistry;

    @Autowired
    @Lazy
    private UdpListenPortRegistry udpListenPortRegistry;

    /**
     * 该本地端口所属的档案（自定义端口与档案一一对应）；未命中返回空。
     */
    public Optional<DeviceProfile> resolveInboundProfileForLocalPort(int localPort) {
        return udpListenPortRegistry == null
                ? Optional.empty()
                : udpListenPortRegistry.profileForListenPort(localPort);
    }

    /**
     * SERVER 模式的下行目的地：设备连接配置里填了固定下行地址（{@code udpDownlinkHost/Port}）就用它，
     * **没配就回发设备最近一次上报的源地址**。
     * <p>
     * 优先用配置地址：设备从临时/NAT 端口上报、另开固定端口收指令时，上报地址的端口对不上
     * （经透明绑定网关接入时上游源端口也由网关决定，只有配置地址是可靠的）。
     * 没配就回退到上报地址 —— 透明绑定下那个地址通常就是设备的真实 IP+端口，能通。
     */
    public InetSocketAddress resolveDownlinkAddress(DeviceId deviceId, InetSocketAddress reportedAddress) {
        InetSocketAddress configured = udpDownlinkAddressRegistry == null
                ? null
                : udpDownlinkAddressRegistry.resolve(deviceId);
        return configured != null ? configured : reportedAddress;
    }

    /**
     * 该设备当前是否有**真实**的入站会话（出站会话不算 —— 那是"设备还没开口时"为了下发而建的，
     * 见 {@code com.jnks.iot.server.transport.udp.outbound.UdpOutboundTransportContext}）。
     */
    public boolean hasActiveServerSession(DeviceId deviceId) {
        if (deviceId == null) {
            return false;
        }
        UdpDeviceSession session = serverSessions.get(deviceId);
        return session != null && session.isConnected();
    }

    /** 该档案声明的自定义监听端口（服务端模式）；CLIENT 模式或没配返回 {@code null}。 */
    public Integer resolveProfileListenPort(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration cfg)) {
            return null;
        }
        return cfg.getUdpProfileServerBindPort();
    }

    public UdpTransportContext(TransportDeviceProfileCache deviceProfileCache,
                               TransportService transportService,
                               UdpProtoTransportEntityService protoEntityService,
                               UdpTransportBalancingService balancingService,
                               UdpSourceBindingService udpSourceBindingService,
                               UdpProtocolDeviceIdRegistry udpProtocolDeviceIdRegistry,
                               UdpDeferredAuthCatalog udpDeferredAuthCatalog,
                               UdpMessageProcessor udpMessageProcessor,
                               @Lazy UdpTransportService udpTransportService) {
        this.deviceProfileCache = deviceProfileCache;
        this.transportService = transportService;
        this.protoEntityService = protoEntityService;
        this.balancingService = balancingService;
        this.udpSourceBindingService = udpSourceBindingService;
        this.udpProtocolDeviceIdRegistry = udpProtocolDeviceIdRegistry;
        this.udpDeferredAuthCatalog = udpDeferredAuthCatalog;
        this.udpMessageProcessor = udpMessageProcessor;
        this.udpTransportService = udpTransportService;
    }
    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void startReadIdleSweeper() {
        long periodSec = 10L;
        scheduler.scheduleAtFixedRate(this::sweepIdleInboundSessions, periodSec, periodSec, TimeUnit.SECONDS);
        log.info("UDP read-idle sweeper started (every {}s)", periodSec);
    }

    /**
     * 预热"已知的 UDP 设备"集合。
     * <p>
     * {@link #onDeviceUpdatedOrCreated} 用它区分"第一次听到某台设备"和"后续更新"，只有后者才会去关它的旧会话。
     * 不预热的话，本实例重启后收到的**第一条**设备更新会被当成"新设备"、直接跳过关闭 ——
     * 于是重启后设备先连上、再改配置时，旧会话不会被关（既不产生 STOPPED，也不通知 Core）。
     */
    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void loadKnownUdpDeviceIds() {
        try {
            int page = 0;
            boolean hasNext;
            do {
                TransportProtos.GetUdpDevicesResponseMsg response = protoEntityService.getUdpDevicesIds(page, 512);
                for (String id : response.getIdsList()) {
                    allUdpDeviceIds.add(new DeviceId(UUID.fromString(id)));
                }
                hasNext = response.getHasNextPage();
                page++;
            } while (hasNext);
            log.info("UDP known device ids loaded: {}", allUdpDeviceIds.size());
        } catch (Exception e) {
            log.warn("Failed to load UDP device ids", e);
        }
    }

    private void failInboundSession(UdpDeviceSession session) {
        session.endServerAuth();
        evictInboundPeerSession(session);
    }

    /**
     * 关闭一个**已通过鉴权**的入站会话：本地清理 + 通知 Core 会话已结束 + 补写 STOPPED 生命周期事件。
     * <p>
     * UDP 没有连接级 close，会话是被"读空闲回收 / 档案端口变更 / 档案或设备更新"关掉的，
     * 这几条路原先只做本地清理（{@link UdpDeviceSession#close()}），Core 收不到
     * {@code SESSION_CLOSED}、{@code lc_event} 里也永远不会出现 STOPPED。
     * {@link #onChannelClosed} 里那套 STOPPED 只对 CLIENT 模式可达，服务端模式（设备主动上报）走不到。
     * <p>
     * **Core 主动要求关会话**（非活跃超时 / 并发上限）时也走这里 —— 见
     * {@link UdpDeviceSession#onRemoteSessionCloseCommand(UUID, TransportProtos.SessionCloseNotificationProto)}：
     * 那条路同样是"真正的会话终结"，不补 STOPPED 的话 STARTED 与 STOPPED 永远配不上对。
     * <p>
     * 鉴权未完成的会话（{@code sessionInfo == null}）只做本地清理，不会凭空产生 STOPPED。
     * <p>
     * 幂等：只在会话**由连通转为关闭**那一次上报。读空闲扫描是每 10 秒一轮的快照遍历，
     * 若会话因别的原因没能从集合里摘掉（见 {@link #untrackInboundSession}），
     * 没有这道闸就会反复给 Core 发 SESSION_CLOSED / STOPPED。
     */
    public void closeRegisteredInboundSession(UdpDeviceSession session) {
        boolean wasConnected = session.isConnected();
        session.close();
        TransportProtos.SessionInfoProto sessionInfo = session.getSessionInfo();
        if (!wasConnected || sessionInfo == null || session.getDeviceId() == null) {
            return;
        }
        transportService.process(sessionInfo, DefaultTransportService.SESSION_EVENT_MSG_CLOSED, null);
        transportService.deregisterSession(sessionInfo);
        transportService.lifecycleEvent(session.getTenantId(), session.getDeviceId(),
                ComponentLifecycleEvent.STOPPED, true, null);
        serverSessions.remove(session.getDeviceId(), session);
    }

    public UdpDeferredAuthCatalog getDeferredAuthCatalog() {
        return udpDeferredAuthCatalog;
    }

    public UdpDeviceSession newInboundDeviceSession() {
        return new UdpDeviceSession(UUID.randomUUID(), this);
    }

    /**
     * 按本地端口 + 对端地址复用 UDP 会话（无连接协议，以 (localPort, remote) 为键）。
     */
    public UdpDeviceSession resolveOrCreateInboundSession(Channel channel, int localPort, InetSocketAddress sender) {
        UdpPeerKey key = UdpPeerKey.of(localPort, sender);
        return inboundSessionByPeer.computeIfAbsent(key, k -> {
            UdpDeviceSession session = newInboundDeviceSession();
            session.setChannel(channel);
            session.setRemoteAddress(sender);
            session.setLastUplinkMs(System.currentTimeMillis());
            // 自定义端口与档案一一对应：鉴权前就绑定档案，后续鉴权/解析都按该档案走，
            // 也避免"按源 IP 认领"把别的档案端口上的报文抢走。
            resolveInboundProfileForLocalPort(localPort).ifPresent(session::setDeviceProfile);
            // 每个 UDP 数据报即一帧：分帧恒为 NONE，无需按端口解析档案
            session.setInboundPipelineFramingMode(UdpTransportFramingMode.NONE);
            session.setInboundPipelineFixedFrameLength(0);
            trackInboundSession(session);
            return session;
        });
    }

    public void afterSuccessfulAuth(ChannelHandlerContext ctx, UdpDeviceSession session, ValidateDeviceCredentialsResponse msg) {
        completeSessionRegistration(session, msg);
        if (session.getDeviceId() != null) {
            UdpDeviceSession oldSession = serverSessions.put(session.getDeviceId(), session);
            if (oldSession != null && oldSession != session) {
                log.info("[{}] Closing previous server session due to new inbound datagram peer", session.getDeviceId());
                oldSession.close();
            }
        }
        session.endServerAuth();
    }

    public void evictInboundPeerSession(UdpDeviceSession session) {
        inboundSessionByPeer.entrySet().removeIf(e -> e.getValue() == session);
        untrackInboundSession(session);
    }

    private void completeSessionRegistration(UdpDeviceSession session, ValidateDeviceCredentialsResponse msg) {
        TransportProtos.SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, this, session.getSessionId());
        transportService.registerAsyncSession(sessionInfo, session);
        transportService.process(sessionInfo, DefaultTransportService.SESSION_EVENT_MSG_OPEN, null);
        // TCP 会话一旦连上即记录一次活动，避免“刚连接就显示 inactive”。
        transportService.recordActivity(sessionInfo);
        transportService.process(sessionInfo, TransportProtos.SubscribeToAttributeUpdatesMsg.newBuilder()
                .setSessionType(TransportProtos.SessionType.ASYNC)
                .build(), TransportServiceCallback.EMPTY);
        transportService.process(sessionInfo, TransportProtos.SubscribeToRPCMsg.newBuilder()
                .setSessionType(TransportProtos.SessionType.ASYNC)
                .build(), TransportServiceCallback.EMPTY);
        session.setSessionInfo(sessionInfo);
        session.setDeviceInfo(msg.getDeviceInfo());
        session.setDeviceProfile(msg.getDeviceProfile());
        session.setCoreSessionReady(true);
        session.setConnected(true);
        transportService.lifecycleEvent(session.getTenantId(), session.getDeviceId(), ComponentLifecycleEvent.STARTED, true, null);
    }
    private static int readIdleSecFromProfile(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration)) {
            return 0;
        }
        return ((UdpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration())
                .getEffectiveUdpReadIdleTimeoutSec();
    }

    /** 档案的链路上鉴权模式；非 UDP 档案按 NONE 兜底（与 {@link UdpDeviceSession} 的取值口径一致）。 */
    private static UdpWireAuthenticationMode wireAuthModeOf(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration ptc)) {
            return UdpWireAuthenticationMode.NONE;
        }
        return ptc.getUdpWireAuthenticationMode();
    }

    /**
     * 读空闲清理：档案配了 {@code udpReadIdleTimeoutSec} 时，超过该秒数没收到该设备的数据报就关掉会话
     * （设备下次发包会重新鉴权建会话）。1 秒以下按 1 秒算，避免配置成 0.x 之类的意外值。
     */
    private void sweepIdleInboundSessions() {
        long now = System.currentTimeMillis();
        for (UdpDeviceSession s : new ArrayList<>(inboundSessions.values())) {
            int idleSec = readIdleSecFromProfile(s.getDeviceProfile());
            if (idleSec <= 0) {
                continue;
            }
            if (now - s.getLastUplinkMs() > idleSec * 1000L) {
                log.info("[{}] Closing UDP session: no datagram for {}s (profile udpReadIdleTimeoutSec)",
                        s.getSessionId(), idleSec);
                closeRegisteredInboundSession(s);
            }
        }
    }

    public void onUdpSessionDeviceDeleted(UdpDeviceSession session) {
        // 与其它关闭路径统一：走 closeRegisteredInboundSession —— 它会通知 Core、注销传输层会话、
        // 并补一条 STOPPED。原先的裸 close() 这三样全漏，设备被删后会留下 Core 侧订阅与一个悬挂的会话。
        closeRegisteredInboundSession(session);
    }
    public void onUdpDeviceProfileUpdated(UdpDeviceSession session, DeviceProfile deviceProfile) {
        session.setDeviceProfile(deviceProfile);
    }
    public void onUdpDeviceUpdated(UdpDeviceSession session, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        deviceProfileOpt.ifPresent(session::setDeviceProfile);
    }

    private void closeServerSessionIfExists(DeviceId deviceId) {
        // 用 get 而不是 remove：摘除由统一关闭路径负责，否则会话已从集合里消失、
        // 后面的 closeInboundSessionsAffectedByDeviceUpdate 也补不上，STOPPED 与 SESSION_CLOSED 都会丢。
        UdpDeviceSession serverSession = serverSessions.get(deviceId);
        if (serverSession != null) {
            log.info("[{}] Closing server session due to device/profile update", deviceId);
            closeRegisteredInboundSession(serverSession);
        }
    }

    /**
     * 登记入站会话（含鉴权完成前），便于在端口解绑或配置变更时主动 {@link UdpDeviceSession#close()}。
     */
    public void trackInboundSession(UdpDeviceSession session) {
        inboundSessions.put(session.getSessionId(), session);
    }

    public void untrackInboundSession(UdpDeviceSession session) {
        inboundSessions.remove(session.getSessionId(), session);
    }

    /**
     * 解绑专用本地端口之前调用：Netty 关闭 {@code ServerChannel} 后，已 accept 的子 TCP 连接仍可能保持打开。
     */
    public void closeInboundSessionsOnLocalPort(int localPort) {
        for (UdpDeviceSession s : new ArrayList<>(inboundSessions.values())) {
            Channel ch = s.getChannel();
            if (ch == null || !ch.isOpen()) {
                continue;
            }
            SocketAddress la = ch.localAddress();
            if (la instanceof InetSocketAddress isa && isa.getPort() == localPort) {
                log.info("[{}] Closing inbound TCP on local port {} (dedicated listen stopping)", s.getSessionId(), localPort);
                closeRegisteredInboundSession(s);
            }
        }
    }

    private void closeInboundSessionsForDeviceProfile(DeviceProfile profile) {
        if (profile == null || profile.getId() == null) {
            return;
        }
        for (UdpDeviceSession s : new ArrayList<>(inboundSessions.values())) {
            DeviceProfile sp = s.getDeviceProfile();
            if (sp != null && profile.getId().equals(sp.getId())) {
                log.info("[{}] Closing inbound TCP due to device profile update ({})", s.getSessionId(), profile.getId());
                closeRegisteredInboundSession(s);
            }
        }
    }

    private void closeInboundSessionsAffectedByDeviceUpdate(Device device) {
        if (device.getDeviceData() == null
                || !(device.getDeviceData().getTransportConfiguration() instanceof UdpDeviceTransportConfiguration)) {
            return;
        }
        var deviceProfileId = device.getDeviceProfileId();
        if (deviceProfileId == null) {
            return;
        }
        for (UdpDeviceSession s : new ArrayList<>(inboundSessions.values())) {
            if (s.getDeviceId() != null && s.getDeviceId().equals(device.getId())) {
                log.info("[{}] Closing inbound TCP due to device update", s.getSessionId());
                closeRegisteredInboundSession(s);
                continue;
            }
            DeviceProfile sp = s.getDeviceProfile();
            if (sp != null && !deviceProfileId.equals(sp.getId())) {
                log.info("[{}] Closing inbound TCP (stale profile vs device after update)", s.getSessionId());
                closeRegisteredInboundSession(s);
            }
        }
    }

    @EventListener(DeviceProfileUpdatedEvent.class)
    public void onDeviceProfileUpdatedForInbound(DeviceProfileUpdatedEvent event) {
        DeviceProfile p = event.getDeviceProfile();
        if (p == null || p.getTransportType() != DeviceTransportType.UDP) {
            return;
        }
        closeInboundSessionsForDeviceProfile(p);
    }

    @EventListener(DeviceUpdatedEvent.class)
    public void onDeviceUpdatedOrCreated(DeviceUpdatedEvent event) {
        Device device = event.getDevice();
        DeviceTransportType transportType = Optional.ofNullable(device.getDeviceData().getTransportConfiguration())
                .map(DeviceTransportConfiguration::getType)
                .orElse(null);
        if (!allUdpDeviceIds.contains(device.getId())) {
            if (transportType != DeviceTransportType.UDP) {
                return;
            }
            allUdpDeviceIds.add(device.getId());
        } else {
            closeServerSessionIfExists(device.getId());
            closeInboundSessionsAffectedByDeviceUpdate(device);
        }
    }

    /**
     * 每收到一帧已分帧的业务数据即记活动，与 JSON/模板解析是否成功无关。
     * 否则 RAW_BYTES / 协议模板未命中时不会走 {@code transportService.process(...)}，设备会一直不活跃。
     */
    public void recordUplinkFrameActivity(UdpDeviceSession session) {
        if (session == null || !session.isCoreSessionReady()) {
            return;
        }
        TransportProtos.SessionInfoProto sessionInfo = session.getSessionInfo();
        if (sessionInfo != null) {
            transportService.recordActivity(sessionInfo);
        }
    }

    public UdpProtoTransportEntityService getProtoEntityService() {
        return protoEntityService;
    }

    /**
     * SERVER 入站：若远端 IP 已绑定且配置文件为 {@link UdpWireAuthenticationMode#NONE}，则在 Core 侧静默校验访问令牌并注册会话。
     *
     * @return true 表示已走异步注册，此时须保持 autoRead=false 直至回调中打开
     */
    public boolean startServerWireAuth(ChannelHandlerContext ctx, UdpDeviceSession session, InetSocketAddress remote) {
        DeviceProfile bound = session.getDeviceProfile();
        // 本端口所属档案不是 NONE 鉴权时，绝不走"按源 IP 认领"：源地址绑定只按 IP 索引、不看端口，
        // 否则同一 IP 发往本档案端口的报文会被别的 NONE 档案设备抢走。
        if (bound != null && wireAuthModeOf(bound) != UdpWireAuthenticationMode.NONE) {
            return false;
        }
        var deviceIdOpt = udpSourceBindingService.findDeviceIdForRemoteAddress(remote);
        if (deviceIdOpt.isEmpty()) {
            return false;
        }
        Device device = protoEntityService.getDeviceById(deviceIdOpt.get());
        if (device == null) {
            return false;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        // 候选设备必须就是本端口所属档案的设备（NONE 档案之间也不能跨档案认领）
        if (bound != null && (profile == null || !bound.getId().equals(profile.getId()))) {
            return false;
        }
        if (!sourceHostMatchesIfRequired(device, remote)) {
            log.warn("[{}] UDP NONE: sourceHost mismatch", device.getId());
            failInboundSession(session);
            return true;
        }
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration)) {
            return false;
        }
        UdpDeviceProfileTransportConfiguration ptc = (UdpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getUdpWireAuthenticationMode() != UdpWireAuthenticationMode.NONE) {
            return false;
        }
        DeviceCredentials cred = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (cred.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            return false;
        }
        session.setDeviceProfile(profile);
        transportService.process(DeviceTransportType.UDP,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(cred.getCredentialsId()).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse response) {
                        if (!response.hasDeviceInfo()) {
                            log.warn("[{}] NONE wire auth: Core rejected credentials", device.getId());
                            failInboundSession(session);
                            return;
                        }
                        ctx.channel().eventLoop().execute(() -> afterSuccessfulAuth(ctx, session, response));
                    }
                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] NONE wire auth error", device.getId(), e);
                        failInboundSession(session);
                    }
                });
        return true;
    }

    /**
     * SERVER {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID}：
     * 在 Core 会话注册前对每一帧按档案解析；本帧无身份字段则丢弃并等待；有字段则以协议设备号
     * （由设备传输配置 {@code udpWireAuthPayloadDeviceId} 定位 TB 设备）取该设备 ACCESS_TOKEN 注册。
     */
    public void completeDeferredWireAuthServerAuth(ChannelHandlerContext ctx, UdpDeviceSession session, byte[] rawFrame) {
        completeDeferredWireAuthServerAuth(ctx, session, rawFrame, session.getDeviceProfile());
    }

    /**
     * 共享端口下鉴权前会话没有档案：由 {@link UdpDeferredAuthCatalog} 按帧里命中的"已配置延迟鉴权键"
     * 反推档案后调用本入口。
     */
    public void completeDeferredWireAuthServerAuth(ChannelHandlerContext ctx, UdpDeviceSession session, byte[] rawFrame,
                                                   DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration ptc)) {
            log.warn("[{}] Deferred payload wire auth requires inbound session bound to device profile",
                    session.getSessionId());
            failInboundSession(session);
            return;
        }
        if (ptc.getUdpWireAuthenticationMode() != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            log.warn("[{}] inbound handler expected deferred payload wire auth mode", session.getSessionId());
            failInboundSession(session);
            return;
        }
        session.setDeviceProfile(profile);
        Optional<String> fieldValueOpt = udpMessageProcessor.extractDeferredWireAuthAccessToken(profile, session, rawFrame);
        if (fieldValueOpt.isEmpty() || StringUtils.isBlank(fieldValueOpt.get())) {
            log.debug("[{}] Deferred wire auth: identity field absent in this frame, waiting for next frame", session.getSessionId());
            return;
        }
        if (!session.tryBeginServerAuth()) {
            log.debug("[{}] Deferred wire auth: validation already in progress, skip this frame", session.getSessionId());
            return;
        }
        String fieldValue = fieldValueOpt.get().trim();
        Optional<DeviceId> deviceIdOpt = udpProtocolDeviceIdRegistry.findByProtocolDeviceId(fieldValue);
        if (deviceIdOpt.isEmpty()) {
            log.warn("[{}] DEFERRED_PAYLOAD_DEVICE_ID: no TB device for payload device id [{}]",
                    session.getSessionId(), fieldValue);
            failInboundSession(session);
            return;
        }
        Device device = protoEntityService.getDeviceById(deviceIdOpt.get());
        if (device == null) {
            log.warn("[{}] DEFERRED_PAYLOAD_DEVICE_ID: resolved device id {} not found", session.getSessionId(), deviceIdOpt.get());
            failInboundSession(session);
            return;
        }
        // 身份已由监听端口 + 负载协议设备 ID 确定，不再校验 sourceHost（NONE 多机同端口仍靠 IP 区分）。
        DeviceCredentials cred = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (cred.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            log.warn("[{}] DEFERRED_PAYLOAD_DEVICE_ID: device {} has no ACCESS_TOKEN credentials", session.getSessionId(), device.getId());
            failInboundSession(session);
            return;
        }
        submitDeferredAccessTokenValidation(ctx, session, rawFrame, cred.getCredentialsId());
    }

    private void submitDeferredAccessTokenValidation(ChannelHandlerContext ctx, UdpDeviceSession session, byte[] rawFrame, String accessToken) {
        transportService.process(DeviceTransportType.UDP,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(accessToken).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        if (!msg.hasDeviceInfo()) {
                            log.warn("[{}] Deferred wire auth: Core rejected credentials", session.getSessionId());
                            failInboundSession(session);
                            return;
                        }
                        ctx.channel().eventLoop().execute(() -> {
                            session.setDeviceInfo(msg.getDeviceInfo());
                            session.setDeviceProfile(msg.getDeviceProfile());
                            session.setDeviceWireAuthenticated(true);
                            afterSuccessfulAuth(ctx, session, msg);
                            udpMessageProcessor.replayDeferredUplinkAfterAuth(session, rawFrame);
                            recordUplinkFrameActivity(session);
                        });
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] Deferred wire auth: validate error", session.getSessionId(), e);
                        failInboundSession(session);
                    }
                });
    }

private boolean sourceHostMatchesIfRequired(Device device, SocketAddress remote) {
        if (device.getDeviceData() == null || !(device.getDeviceData().getTransportConfiguration() instanceof UdpDeviceTransportConfiguration)) {
            return true;
        }
        UdpDeviceTransportConfiguration dtc = (UdpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        if (StringUtils.isBlank(dtc.getSourceHost())) {
            return true;
        }
        if (!(remote instanceof InetSocketAddress)) {
            return false;
        }
        try {
            String expected = InetAddress.getByName(dtc.getSourceHost().trim()).getHostAddress();
            String actual = ((InetSocketAddress) remote).getAddress().getHostAddress();
            return expected.equals(actual);
        } catch (UnknownHostException e) {
            log.warn("[{}] Invalid sourceHost {}", device.getId(), dtc.getSourceHost());
            return false;
        }
    }

}