/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.udp;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.transport.udp.netty.UdpPipelineBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.thingsboard.server.queue.scheduler.SchedulerComponent;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.transport.udp.service.UdpDedicatedListenPortService;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.UdpEffectiveServerBindPort;
import org.thingsboard.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode;
import org.thingsboard.server.transport.udp.service.UdpProtoTransportEntityService;
import org.thingsboard.server.transport.udp.service.UdpSourceBindingService;
import org.thingsboard.server.common.data.device.profile.UdpTransportConnectMode;
import org.thingsboard.server.common.data.device.profile.UdpTransportFramingMode;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.data.security.DeviceCredentialsType;
import org.thingsboard.server.common.transport.DeviceProfileUpdatedEvent;
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.auth.SessionInfoCreator;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.common.transport.service.DefaultTransportService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.queue.util.TbUdpTransportComponent;
import org.thingsboard.server.transport.udp.event.UdpTransportListChangedEvent;
import org.thingsboard.server.transport.udp.session.UdpDeviceSession;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
@TbUdpTransportComponent
@Component
@Slf4j
public class UdpTransportContext extends org.thingsboard.server.common.transport.TransportContext {
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final UdpProtoTransportEntityService protoEntityService;
    private final UdpTransportBalancingService balancingService;
    private final UdpSourceBindingService udpSourceBindingService;
    private final UdpDedicatedListenPortService udpDedicatedListenPortService;
    @Getter
    private final UdpMessageProcessor udpMessageProcessor;

    private final UdpTransportService udpTransportService;

    private final Map<DeviceId, UdpDeviceSession> clientSessions = new ConcurrentHashMap<>();
    private final Map<DeviceId, UdpDeviceSession> serverSessions = new ConcurrentHashMap<>();
    private final Collection<DeviceId> allUdpDeviceIds = new ConcurrentLinkedDeque<>();
    private final Map<DeviceId, AtomicInteger> clientReconnectFailureCount = new ConcurrentHashMap<>();
    private final Map<DeviceId, ScheduledFuture<?>> clientReconnectTasks = new ConcurrentHashMap<>();

    /**
     * 所有入站（SERVER）会话：含尚未写入 {@link #serverSessions} 的鉴权中连接。
     * 用于在专用监听端口解绑或设备/档案变更时主动断开；仅关闭 Netty 的 ServerChannel 不会自动关闭已接受的子 TCP 连接。
     */
    private final Set<UdpDeviceSession> inboundSessions = ConcurrentHashMap.newKeySet();

    private record UdpPeerKey(int localPort, String host, int port) {
        static UdpPeerKey of(int localPort, InetSocketAddress remote) {
            return new UdpPeerKey(localPort, remote.getAddress().getHostAddress(), remote.getPort());
        }
    }

    private final ConcurrentHashMap<UdpPeerKey, UdpDeviceSession> inboundSessionByPeer = new ConcurrentHashMap<>();

    @Autowired
    private SchedulerComponent scheduler;


    public UdpTransportContext(TransportDeviceProfileCache deviceProfileCache,
                               TransportService transportService,
                               UdpProtoTransportEntityService protoEntityService,
                               UdpTransportBalancingService balancingService,
                               UdpSourceBindingService udpSourceBindingService,
                               UdpDedicatedListenPortService udpDedicatedListenPortService,
                               UdpMessageProcessor udpMessageProcessor,
                               @Lazy UdpTransportService udpTransportService) {
        this.deviceProfileCache = deviceProfileCache;
        this.transportService = transportService;
        this.protoEntityService = protoEntityService;
        this.balancingService = balancingService;
        this.udpSourceBindingService = udpSourceBindingService;
        this.udpDedicatedListenPortService = udpDedicatedListenPortService;
        this.udpMessageProcessor = udpMessageProcessor;
        this.udpTransportService = udpTransportService;
    }
    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void fetchDevicesAndEstablishClientSessions() {
        // UDP 无平台主动 outbound 建连；设备向档案监听端口发数据报即可。
    }
    private boolean isClientProfile(Device device) {
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return false;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof UdpDeviceProfileTransportConfiguration) {
            UdpDeviceProfileTransportConfiguration udpCfg = (UdpDeviceProfileTransportConfiguration) tc;
            return udpCfg.getUdpTransportConnectMode() == UdpTransportConnectMode.CLIENT;
        }
        return false;
    }

    private void failInboundSession(UdpDeviceSession session) {
        session.endServerAuth();
        evictInboundPeerSession(session);
    }
    public UdpDeviceSession newInboundDeviceSession() {
        return new UdpDeviceSession(UUID.randomUUID(), this, false);
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
            Optional<UdpInboundPipelineConfig> dedicatedCfg = resolveInboundPipelineConfigForLocalPort(localPort);
            UdpTransportFramingMode framingMode = UdpTransportFramingMode.NONE;
            int fixedLen = 0;
            if (dedicatedCfg.isPresent()) {
                UdpInboundPipelineConfig cfg = dedicatedCfg.get();
                framingMode = cfg.getFramingMode();
                fixedLen = cfg.getFixedFrameLength();
                session.setDeviceProfile(cfg.getDeviceProfile());
            }
            session.setInboundPipelineFramingMode(framingMode);
            session.setInboundPipelineFixedFrameLength(fixedLen);
            trackInboundSession(session);
            return session;
        });
    }

    public void afterSuccessfulAuth(ChannelHandlerContext ctx, UdpDeviceSession session, ValidateDeviceCredentialsResponse msg) {
        if (!validateDedicatedListenPortIfConfigured(ctx, msg)) {
            session.endServerAuth();
            evictInboundPeerSession(session);
            return;
        }
        completeSessionRegistration(session, msg);
        if (!session.isOutboundClient() && session.getDeviceId() != null) {
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
    /**
     * 出站 CLIENT：在 TCP 已激活后向 Core 注册会话并加入 {@link #clientSessions}。
     */
    public void finishOutboundUdpClientRegistration(UdpDeviceSession session) {
        if (!session.isOutboundClient() || session.getSessionInfo() != null) {
            return;
        }
        ValidateDeviceCredentialsResponse msg = session.takePendingOutboundCredentials();
        if (msg == null || !msg.hasDeviceInfo()) {
            return;
        }
        completeSessionRegistration(session, msg);
        clientSessions.put(session.getDeviceId(), session);
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
    private void establishClientDeviceSession(Device device) {
        if (device == null) {
            return;
        }
        cancelClientReconnectTask(device.getId());
        log.info("Establishing UDP CLIENT session for device {}", device.getId());
        DeviceProfile deviceProfile = deviceProfileCache.get(device.getDeviceProfileId());
        DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (credentials.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            log.warn("[{}] Expected ACCESS_TOKEN credentials", device.getId());
            return;
        }
        UdpDeviceTransportConfiguration deviceCfg = (UdpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        UdpDeviceSession session = new UdpDeviceSession(UUID.randomUUID(), this, true);
        session.setDeviceProfile(deviceProfile);
        transportService.process(DeviceTransportType.UDP,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(credentials.getCredentialsId()).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        if (msg.hasDeviceInfo()) {
                            session.setDeviceInfo(msg.getDeviceInfo());
                            session.setDeviceProfile(msg.getDeviceProfile());
                            session.stashPendingOutboundCredentials(msg);
                            openOutboundConnection(device.getId(), session, deviceCfg.getHost(), deviceCfg.getPort());
                        } else {
                            log.warn("[{}] TCP client auth failed", device.getId());
                        }
                    }
                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] TCP client auth error", device.getId(), e);
                        transportService.lifecycleEvent(device.getTenantId(), device.getId(), ComponentLifecycleEvent.STARTED, false, e);
                    }
                });
    }
    private void openOutboundConnection(DeviceId deviceId, UdpDeviceSession session, String host, int port) {
        int readIdleSec = readIdleSecFromProfile(session.getDeviceProfile());
        Bootstrap b = new Bootstrap();
        b.group(udpTransportService.getWorkerGroup())
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<io.netty.channel.socket.DatagramChannel>() {
                    @Override
                    protected void initChannel(io.netty.channel.socket.DatagramChannel ch) {
                        UdpPipelineBuilder.installReadIdleHandlerFirst(ch.pipeline(), readIdleSec);
                        ch.pipeline().addLast(UdpPipelineBuilder.INBOUND_HANDLER_NAME,
                                new UdpClientInboundHandler(UdpTransportContext.this, session, udpTransportService));
                    }
                });
        b.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                log.error("[{}] Outbound UDP connect failed to {}:{}", deviceId, host, port, future.cause());
                transportService.errorEvent(session.getTenantId(), deviceId, "udpClientConnect", future.cause());
                onChannelClosed(session);
            }
        });
    }

    private static int readIdleSecFromProfile(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration)) {
            return 0;
        }
        return ((UdpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration())
                .getEffectiveUdpReadIdleTimeoutSec();
    }

    public void resetClientReconnectFailureCount(DeviceId deviceId) {
        clientReconnectFailureCount.remove(deviceId);
    }

    private void cancelClientReconnectTask(DeviceId deviceId) {
        ScheduledFuture<?> f = clientReconnectTasks.remove(deviceId);
        if (f != null) {
            f.cancel(false);
        }
    }

    private void scheduleClientReconnect(DeviceId deviceId) {
        if (!balancingService.isManagedByCurrentTransport(deviceId.getId())) {
            return;
        }
        Device device = protoEntityService.getDeviceById(deviceId);
        if (device == null) {
            return;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration)) {
            return;
        }
        UdpDeviceProfileTransportConfiguration cfg = (UdpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (cfg.getUdpTransportConnectMode() != UdpTransportConnectMode.CLIENT) {
            return;
        }
        if (cfg.isUdpOutboundReconnectDisabled()) {
            return;
        }
        int maxAttempts = cfg.getEffectiveUdpOutboundReconnectMaxAttempts();
        if (maxAttempts > 0) {
            int n = clientReconnectFailureCount.computeIfAbsent(deviceId, d -> new AtomicInteger(0)).incrementAndGet();
            if (n > maxAttempts) {
                log.warn("[{}] TCP outbound reconnect stopped after {} failure(s) (max {})", deviceId, n, maxAttempts);
                clientReconnectFailureCount.remove(deviceId);
                return;
            }
        }
        int intervalSec = cfg.getEffectiveUdpOutboundReconnectIntervalSec();
        cancelClientReconnectTask(deviceId);
        ScheduledFuture<?> scheduled = scheduler.schedule(() -> {
            try {
                Device reloaded = protoEntityService.getDeviceById(deviceId);
                if (reloaded != null && isClientProfile(reloaded)) {
                    establishClientDeviceSession(reloaded);
                }
            } catch (Exception e) {
                log.warn("[{}] TCP outbound reconnect task failed", deviceId, e);
            } finally {
                clientReconnectTasks.remove(deviceId);
            }
        }, intervalSec, TimeUnit.SECONDS);
        clientReconnectTasks.put(deviceId, scheduled);
        log.info("[{}] Scheduled TCP outbound reconnect in {} s", deviceId, intervalSec);
    }

    public void onChannelClosed(UdpDeviceSession session) {
        if (!session.isOutboundClient()) {
            untrackInboundSession(session);
        }
        TransportProtos.SessionInfoProto sessionInfo = session.getSessionInfo();
        if (sessionInfo != null) {
            transportService.process(sessionInfo, DefaultTransportService.SESSION_EVENT_MSG_CLOSED, null);
            transportService.deregisterSession(sessionInfo);
            transportService.lifecycleEvent(session.getTenantId(), session.getDeviceId(), ComponentLifecycleEvent.STOPPED, true, null);
        }
        if (session.getDeviceId() != null) {
            clientSessions.remove(session.getDeviceId());
            serverSessions.remove(session.getDeviceId(), session);
        }
        session.setConnected(false);
        session.endServerAuth();
        if (session.isOutboundClient() && session.getDeviceId() != null) {
            scheduleClientReconnect(session.getDeviceId());
        }
    }
    public void onUdpSessionDeviceDeleted(UdpDeviceSession session) {
        session.close();
    }
    public void onUdpDeviceProfileUpdated(UdpDeviceSession session, DeviceProfile deviceProfile) {
        session.setDeviceProfile(deviceProfile);
    }
    public void onUdpDeviceUpdated(UdpDeviceSession session, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        deviceProfileOpt.ifPresent(session::setDeviceProfile);
    }

    private void closeServerSessionIfExists(DeviceId deviceId) {
        UdpDeviceSession serverSession = serverSessions.remove(deviceId);
        if (serverSession != null) {
            log.info("[{}] Closing server session due to device/profile update", deviceId);
            serverSession.close();
        }
    }

    /**
     * 登记入站会话（含鉴权完成前），便于在端口解绑或配置变更时主动 {@link UdpDeviceSession#close()}。
     */
    public void trackInboundSession(UdpDeviceSession session) {
        inboundSessions.add(session);
    }

    public void untrackInboundSession(UdpDeviceSession session) {
        inboundSessions.remove(session);
    }

    /**
     * 解绑专用本地端口之前调用：Netty 关闭 {@code ServerChannel} 后，已 accept 的子 TCP 连接仍可能保持打开。
     */
    public void closeInboundSessionsOnLocalPort(int localPort) {
        for (UdpDeviceSession s : new ArrayList<>(inboundSessions)) {
            Channel ch = s.getChannel();
            if (ch == null || !ch.isOpen()) {
                continue;
            }
            SocketAddress la = ch.localAddress();
            if (la instanceof InetSocketAddress isa && isa.getPort() == localPort) {
                log.info("[{}] Closing inbound TCP on local port {} (dedicated listen stopping)", s.getSessionId(), localPort);
                s.close();
            }
        }
    }

    private void closeInboundSessionsForDeviceProfile(DeviceProfile profile) {
        if (profile == null || profile.getId() == null) {
            return;
        }
        for (UdpDeviceSession s : new ArrayList<>(inboundSessions)) {
            DeviceProfile sp = s.getDeviceProfile();
            if (sp != null && profile.getId().equals(sp.getId())) {
                log.info("[{}] Closing inbound TCP due to device profile update ({})", s.getSessionId(), profile.getId());
                s.close();
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
        for (UdpDeviceSession s : new ArrayList<>(inboundSessions)) {
            if (s.getDeviceId() != null && s.getDeviceId().equals(device.getId())) {
                log.info("[{}] Closing inbound TCP due to device update", s.getSessionId());
                s.close();
                continue;
            }
            DeviceProfile sp = s.getDeviceProfile();
            if (sp != null && !deviceProfileId.equals(sp.getId())) {
                log.info("[{}] Closing inbound TCP (stale profile vs device after update)", s.getSessionId());
                s.close();
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
            if (balancingService.isManagedByCurrentTransport(device.getId().getId()) && isClientProfile(device)) {
                establishClientDeviceSession(device);
            }
        } else {
            if (balancingService.isManagedByCurrentTransport(device.getId().getId())) {
                UdpDeviceSession session = clientSessions.get(device.getId());
                if (transportType == DeviceTransportType.UDP && isClientProfile(device)) {
                    if (session != null) {
                        session.close();
                        clientSessions.remove(device.getId());
                    }
                    establishClientDeviceSession(device);
                } else if (session != null) {
                    session.close();
                    clientSessions.remove(device.getId());
                }
            }
            closeServerSessionIfExists(device.getId());
            closeInboundSessionsAffectedByDeviceUpdate(device);
        }
    }

    @EventListener
    public void onTcpTransportListChanged(UdpTransportListChangedEvent event) {
        log.trace("UDP transport list changed, refreshing client sessions");
        List<DeviceId> deleted = new LinkedList<>();
        for (DeviceId deviceId : allUdpDeviceIds) {
            if (balancingService.isManagedByCurrentTransport(deviceId.getId())) {
                if (!clientSessions.containsKey(deviceId)) {
                    Device device = protoEntityService.getDeviceById(deviceId);
                    if (device != null && isClientProfile(device)) {
                        establishClientDeviceSession(device);
                    } else {
                        deleted.add(deviceId);
                    }
                }
            } else {
                Optional.ofNullable(clientSessions.remove(deviceId)).ifPresent(UdpDeviceSession::close);
            }
        }
        allUdpDeviceIds.removeAll(deleted);
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
    public Collection<UdpDeviceSession> getClientSessions() {
        return clientSessions.values();
    }


    /**
     * SERVER 入站：若远端 IP 已绑定且配置文件为 {@link UdpWireAuthenticationMode#NONE}，则在 Core 侧静默校验访问令牌并注册会话。
     *
     * @return true 表示已走异步注册，此时须保持 autoRead=false 直至回调中打开
     */
    public boolean startServerWireAuth(ChannelHandlerContext ctx, UdpDeviceSession session, InetSocketAddress remote) {
        var deviceIdOpt = udpDedicatedListenPortService.findDeviceIdForDedicatedPortNoneSilentAuth(
                ctx.channel().localAddress(), remote);
        if (deviceIdOpt.isEmpty()) {
            deviceIdOpt = udpSourceBindingService.findDeviceIdForRemoteAddress(remote);
        }
        if (deviceIdOpt.isEmpty()) {
            return false;
        }
        Device device = protoEntityService.getDeviceById(deviceIdOpt.get());
        if (device == null) {
            return false;
        }
        if (!sourceHostMatchesIfRequired(device, remote)) {
            log.warn("[{}] UDP NONE: sourceHost mismatch", device.getId());
            failInboundSession(session);
            return true;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
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
     * SERVER {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_TOKEN} / {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID}：
     * 在 Core 会话注册前对每一帧按档案解析；本帧无身份字段则丢弃并等待；有字段则提交 Core 注册（TOKEN 模式字段值为 ACCESS_TOKEN；
     * DEVICE_ID 模式字段值为协议设备 ID，由监听端口 + 设备传输配置 {@code udpWireAuthPayloadDeviceId} 定位 TB 设备后以该设备 ACCESS_TOKEN 注册）。
     */
    public void completeDeferredWireAuthServerAuth(ChannelHandlerContext ctx, UdpDeviceSession session, byte[] rawFrame) {
        DeviceProfile profile = session.getDeviceProfile();
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration ptc)) {
            log.warn("[{}] Deferred payload wire auth requires inbound session bound to device profile (use serverBindPort dedicated listen)",
                    session.getSessionId());
            failInboundSession(session);
            return;
        }
        UdpWireAuthenticationMode mode = ptc.getUdpWireAuthenticationMode();
        if (mode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN
                && mode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            log.warn("[{}] inbound handler expected deferred payload wire auth mode", session.getSessionId());
            failInboundSession(session);
            return;
        }
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
        if (mode == UdpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN) {
            submitDeferredAccessTokenValidation(ctx, session, rawFrame, fieldValue);
            return;
        }
        int localPort = ((InetSocketAddress) ctx.channel().localAddress()).getPort();
        Optional<DeviceId> deviceIdOpt = udpDedicatedListenPortService.findDeviceIdByListenPortAndProtocolDeviceId(localPort, fieldValue);
        if (deviceIdOpt.isEmpty()) {
            log.warn("[{}] DEFERRED_PAYLOAD_DEVICE_ID: no TB device for local port {} and payload device id [{}]",
                    session.getSessionId(), localPort, fieldValue);
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

    private boolean validateDedicatedListenPortIfConfigured(ChannelHandlerContext ctx, ValidateDeviceCredentialsResponse msg) {
        if (!msg.hasDeviceInfo()) {
            return true;
        }
        var di = msg.getDeviceInfo();
        DeviceId deviceId = di.getDeviceId();
        if (deviceId == null) {
            return true;
        }
        Device device = protoEntityService.getDeviceById(deviceId);
        if (device == null || device.getDeviceData() == null
                || !(device.getDeviceData().getTransportConfiguration() instanceof UdpDeviceTransportConfiguration)) {
            return true;
        }
        UdpDeviceTransportConfiguration dtc = (UdpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        Integer expectedPort = UdpEffectiveServerBindPort.resolve(profile, dtc);
        if (expectedPort == null) {
            return true;
        }
        int localPort = ((InetSocketAddress) ctx.channel().localAddress()).getPort();
        if (localPort != expectedPort) {
            log.warn("[{}] TCP auth rejected: expect listen port {} but socket local port is {}", deviceId, expectedPort, localPort);
            return false;
        }
        return true;
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


    /**
     * 专用监听端口（设备 {@code serverBindPort}）上入站时，从设备配置文件解析首段分帧与负载类型（与 {@link UdpDeviceProfileTransportConfiguration} 一致）。
     */
    public Optional<UdpInboundPipelineConfig> resolveInboundPipelineConfigForLocalPort(int localPort) {
        Optional<DeviceId> idOpt = udpDedicatedListenPortService.findAnyDeviceIdForLocalPort(localPort);
        if (idOpt.isEmpty()) {
            return Optional.empty();
        }
        Device device = protoEntityService.getDeviceById(idOpt.get());
        if (device == null) {
            return Optional.empty();
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration)) {
            return Optional.empty();
        }
        UdpDeviceProfileTransportConfiguration udpCfg = (UdpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (udpCfg.getUdpProfileServerBindPort() == null) {
            return Optional.empty();
        }
        return Optional.of(new UdpInboundPipelineConfig(UdpTransportFramingMode.NONE, 0, profile));
    }
}