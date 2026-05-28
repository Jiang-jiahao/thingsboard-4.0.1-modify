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
package org.thingsboard.server.transport.tcp;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.transport.tcp.netty.TcpPipelineBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.thingsboard.server.queue.scheduler.SchedulerComponent;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.transport.tcp.service.TcpDedicatedListenPortService;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.TcpDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.TcpEffectiveServerBindPort;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode;
import org.thingsboard.server.transport.tcp.service.TcpProtoTransportEntityService;
import org.thingsboard.server.transport.tcp.service.TcpSourceBindingService;
import org.thingsboard.server.common.data.device.profile.TcpTransportConnectMode;
import org.thingsboard.server.common.data.device.profile.TcpTransportFramingMode;
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
import org.thingsboard.server.queue.util.TbTcpTransportComponent;
import org.thingsboard.server.transport.tcp.event.TcpTransportListChangedEvent;
import org.thingsboard.server.transport.tcp.session.TcpDeviceSession;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
@TbTcpTransportComponent
@Component
@Slf4j
public class TcpTransportContext extends org.thingsboard.server.common.transport.TransportContext {
    private final TransportDeviceProfileCache deviceProfileCache;
    private final TransportService transportService;
    private final TcpProtoTransportEntityService protoEntityService;
    private final TcpTransportBalancingService balancingService;
    private final TcpSourceBindingService tcpSourceBindingService;
    private final TcpDedicatedListenPortService tcpDedicatedListenPortService;
    @Getter
    private final TcpMessageProcessor tcpMessageProcessor;

    private final TcpTransportService tcpTransportService;

    private final Map<DeviceId, TcpDeviceSession> clientSessions = new ConcurrentHashMap<>();
    private final Map<DeviceId, TcpDeviceSession> serverSessions = new ConcurrentHashMap<>();
    private final Collection<DeviceId> allTcpDeviceIds = new ConcurrentLinkedDeque<>();
    private final Map<DeviceId, AtomicInteger> clientReconnectFailureCount = new ConcurrentHashMap<>();
    private final Map<DeviceId, ScheduledFuture<?>> clientReconnectTasks = new ConcurrentHashMap<>();

    /**
     * 所有入站（SERVER）会话：含尚未写入 {@link #serverSessions} 的鉴权中连接。
     * 用于在专用监听端口解绑或设备/档案变更时主动断开；仅关闭 Netty 的 ServerChannel 不会自动关闭已接受的子 TCP 连接。
     */
    private final Set<TcpDeviceSession> inboundSessions = ConcurrentHashMap.newKeySet();

    @Autowired
    private SchedulerComponent scheduler;


    public TcpTransportContext(TransportDeviceProfileCache deviceProfileCache,
                               TransportService transportService,
                               TcpProtoTransportEntityService protoEntityService,
                               TcpTransportBalancingService balancingService,
                               TcpSourceBindingService tcpSourceBindingService,
                               TcpDedicatedListenPortService tcpDedicatedListenPortService,
                               TcpMessageProcessor tcpMessageProcessor,
                               @Lazy TcpTransportService tcpTransportService) {
        this.deviceProfileCache = deviceProfileCache;
        this.transportService = transportService;
        this.protoEntityService = protoEntityService;
        this.balancingService = balancingService;
        this.tcpSourceBindingService = tcpSourceBindingService;
        this.tcpDedicatedListenPortService = tcpDedicatedListenPortService;
        this.tcpMessageProcessor = tcpMessageProcessor;
        this.tcpTransportService = tcpTransportService;
    }
    @AfterStartUp(order = AfterStartUp.AFTER_TRANSPORT_SERVICE)
    public void fetchDevicesAndEstablishClientSessions() {
        log.info("Initializing TCP CLIENT mode device sessions");
        int batchIndex = 0;
        int batchSize = 512;
        boolean nextBatchExists = true;
        while (nextBatchExists) {
            TransportProtos.GetTcpDevicesResponseMsg response = protoEntityService.getTcpDevicesIds(batchIndex, batchSize);
            response.getIdsList().stream()
                    .map(id -> new DeviceId(UUID.fromString(id)))
                    .peek(allTcpDeviceIds::add)
                    .filter(deviceId -> balancingService.isManagedByCurrentTransport(deviceId.getId()))
                    .map(protoEntityService::getDeviceById)
                    .forEach(device -> getExecutor().execute(() -> {
                        if (device != null && isClientProfile(device)) {
                            establishClientDeviceSession(device);
                        }
                    }));
            nextBatchExists = response.getHasNextPage();
            batchIndex++;
        }
    }
    private boolean isClientProfile(Device device) {
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return false;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof TcpDeviceProfileTransportConfiguration) {
            TcpDeviceProfileTransportConfiguration tcpCfg = (TcpDeviceProfileTransportConfiguration) tc;
            return tcpCfg.getTcpTransportConnectMode() == TcpTransportConnectMode.CLIENT;
        }
        return false;
    }
    public TcpDeviceSession newInboundDeviceSession() {
        return new TcpDeviceSession(UUID.randomUUID(), this, false);
    }
    public void afterSuccessfulAuth(ChannelHandlerContext ctx, TcpDeviceSession session, ValidateDeviceCredentialsResponse msg) {
        if (!validateDedicatedListenPortIfConfigured(ctx, msg)) {
            session.endServerAuth();
            ctx.close();
            return;
        }
        completeSessionRegistration(session, msg);
        if (!session.isOutboundClient() && session.getDeviceId() != null) {
            TcpDeviceSession oldSession = serverSessions.put(session.getDeviceId(), session);
            if (oldSession != null && oldSession != session) {
                log.info("[{}] Closing previous server session due to new inbound connection", session.getDeviceId());
                oldSession.close();
            }
        }
        session.endServerAuth();
        int readIdleSec = readIdleSecFromProfile(session.getDeviceProfile());
        ctx.channel().eventLoop().execute(() -> {
            TcpPipelineBuilder.replaceFramingIfNeeded(ctx.pipeline(),
                    session.getInboundPipelineFramingMode() != null ? session.getInboundPipelineFramingMode() : tcpTransportService.getServerAuthFramingMode(),
                    session.getInboundPipelineFramingMode() != null ? session.getInboundPipelineFixedFrameLength() : tcpTransportService.getServerAuthFixedFrameLength(),
                    session.getTcpTransportFramingMode(),
                    session.getTcpFixedFrameLengthForFraming(),
                    tcpTransportService.getMaxFrameLength());
            TcpPipelineBuilder.installReadIdleHandlerFirst(ctx.pipeline(), readIdleSec);
        });
    }
    /**
     * 出站 CLIENT：在 TCP 已激活后向 Core 注册会话并加入 {@link #clientSessions}。
     */
    public void finishOutboundTcpClientRegistration(TcpDeviceSession session) {
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

    private void completeSessionRegistration(TcpDeviceSession session, ValidateDeviceCredentialsResponse msg) {
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
        log.info("Establishing TCP CLIENT session for device {}", device.getId());
        DeviceProfile deviceProfile = deviceProfileCache.get(device.getDeviceProfileId());
        DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (credentials.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            log.warn("[{}] Expected ACCESS_TOKEN credentials", device.getId());
            return;
        }
        TcpDeviceTransportConfiguration deviceCfg = (TcpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        TcpDeviceSession session = new TcpDeviceSession(UUID.randomUUID(), this, true);
        session.setDeviceProfile(deviceProfile);
        transportService.process(DeviceTransportType.TCP,
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
    private void openOutboundConnection(DeviceId deviceId, TcpDeviceSession session, String host, int port) {
        int readIdleSec = readIdleSecFromProfile(session.getDeviceProfile());
        Bootstrap b = new Bootstrap();
        b.group(tcpTransportService.getWorkerGroup())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        TcpPipelineBuilder.installReadIdleHandlerFirst(ch.pipeline(), readIdleSec);
                        TcpPipelineBuilder.addFramingFirst(ch.pipeline(),
                                session.getTcpTransportFramingMode(),
                                tcpTransportService.getMaxFrameLength(),
                                session.getTcpFixedFrameLengthForFraming());
                        ch.pipeline().addLast(TcpPipelineBuilder.INBOUND_HANDLER_NAME,
                                new TcpInboundHandler(TcpTransportContext.this, session, true));
                    }
                });
        b.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                log.error("[{}] Outbound TCP connect failed to {}:{}", deviceId, host, port, future.cause());
                transportService.errorEvent(session.getTenantId(), deviceId, "tcpClientConnect", future.cause());
                onChannelClosed(session, future.cause());
            }
        });
    }

    private static int readIdleSecFromProfile(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration)) {
            return 0;
        }
        return ((TcpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration())
                .getEffectiveTcpReadIdleTimeoutSec();
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
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration)) {
            return;
        }
        TcpDeviceProfileTransportConfiguration cfg = (TcpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (cfg.getTcpTransportConnectMode() != TcpTransportConnectMode.CLIENT) {
            return;
        }
        if (cfg.isTcpOutboundReconnectDisabled()) {
            return;
        }
        int maxAttempts = cfg.getEffectiveTcpOutboundReconnectMaxAttempts();
        if (maxAttempts > 0) {
            int n = clientReconnectFailureCount.computeIfAbsent(deviceId, d -> new AtomicInteger(0)).incrementAndGet();
            if (n > maxAttempts) {
                log.warn("[{}] TCP outbound reconnect stopped after {} failure(s) (max {})", deviceId, n, maxAttempts);
                clientReconnectFailureCount.remove(deviceId);
                return;
            }
        }
        int intervalSec = cfg.getEffectiveTcpOutboundReconnectIntervalSec();
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

    public void onChannelClosed(TcpDeviceSession session, Throwable cause) {
        if (!session.beginCloseHandling()) {
            return;
        }
        if (!session.isOutboundClient()) {
            untrackInboundSession(session);
        }
        recordTcpSessionEndEvent(session, cause);
        TransportProtos.SessionInfoProto sessionInfo = session.getSessionInfo();
        if (sessionInfo != null) {
            transportService.process(sessionInfo, DefaultTransportService.SESSION_EVENT_MSG_CLOSED, null);
            transportService.deregisterSession(sessionInfo);
            transportService.lifecycleEvent(session.getTenantId(), session.getDeviceId(), ComponentLifecycleEvent.STOPPED, true,
                    cause != null ? cause : null);
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

    /** 仅在实际异常时写入 ERROR；正常断开由 lifecycle STOPPED 记录。 */
    private void recordTcpSessionEndEvent(TcpDeviceSession session, Throwable cause) {
        if (cause == null || session.getDeviceId() == null || session.getTenantId() == null) {
            return;
        }
        transportService.errorEvent(session.getTenantId(), session.getDeviceId(), "tcpChannelError", cause);
    }

    public void onTcpSessionDeviceDeleted(TcpDeviceSession session) {
        session.close();
    }
    public void onTcpDeviceProfileUpdated(TcpDeviceSession session, DeviceProfile deviceProfile) {
        session.setDeviceProfile(deviceProfile);
    }
    public void onTcpDeviceUpdated(TcpDeviceSession session, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        deviceProfileOpt.ifPresent(session::setDeviceProfile);
    }

    private void closeServerSessionIfExists(DeviceId deviceId) {
        TcpDeviceSession serverSession = serverSessions.remove(deviceId);
        if (serverSession != null) {
            log.info("[{}] Closing server session due to device/profile update", deviceId);
            serverSession.close();
        }
    }

    /**
     * 登记入站会话（含鉴权完成前），便于在端口解绑或配置变更时主动 {@link TcpDeviceSession#close()}。
     */
    public void trackInboundSession(TcpDeviceSession session) {
        inboundSessions.add(session);
    }

    public void untrackInboundSession(TcpDeviceSession session) {
        inboundSessions.remove(session);
    }

    /**
     * 解绑专用本地端口之前调用：Netty 关闭 {@code ServerChannel} 后，已 accept 的子 TCP 连接仍可能保持打开。
     */
    public void closeInboundSessionsOnLocalPort(int localPort) {
        for (TcpDeviceSession s : new ArrayList<>(inboundSessions)) {
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
        for (TcpDeviceSession s : new ArrayList<>(inboundSessions)) {
            DeviceProfile sp = s.getDeviceProfile();
            if (sp != null && profile.getId().equals(sp.getId())) {
                log.info("[{}] Closing inbound TCP due to device profile update ({})", s.getSessionId(), profile.getId());
                s.close();
            }
        }
    }

    private void closeInboundSessionsAffectedByDeviceUpdate(Device device) {
        if (device.getDeviceData() == null
                || !(device.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration)) {
            return;
        }
        var deviceProfileId = device.getDeviceProfileId();
        if (deviceProfileId == null) {
            return;
        }
        for (TcpDeviceSession s : new ArrayList<>(inboundSessions)) {
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
        if (p == null || p.getTransportType() != DeviceTransportType.TCP) {
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
        if (!allTcpDeviceIds.contains(device.getId())) {
            if (transportType != DeviceTransportType.TCP) {
                return;
            }
            allTcpDeviceIds.add(device.getId());
            if (balancingService.isManagedByCurrentTransport(device.getId().getId()) && isClientProfile(device)) {
                establishClientDeviceSession(device);
            }
        } else {
            if (balancingService.isManagedByCurrentTransport(device.getId().getId())) {
                TcpDeviceSession session = clientSessions.get(device.getId());
                if (transportType == DeviceTransportType.TCP && isClientProfile(device)) {
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
    public void onTcpTransportListChanged(TcpTransportListChangedEvent event) {
        log.trace("TCP transport list changed, refreshing client sessions");
        List<DeviceId> deleted = new LinkedList<>();
        for (DeviceId deviceId : allTcpDeviceIds) {
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
                Optional.ofNullable(clientSessions.remove(deviceId)).ifPresent(TcpDeviceSession::close);
            }
        }
        allTcpDeviceIds.removeAll(deleted);
    }
    /**
     * 每收到一帧已分帧的业务数据即记活动，与 JSON/模板解析是否成功无关。
     * 否则 RAW_BYTES / 协议模板未命中时不会走 {@code transportService.process(...)}，设备会一直不活跃。
     */
    public void recordUplinkFrameActivity(TcpDeviceSession session) {
        if (session == null || !session.isCoreSessionReady()) {
            return;
        }
        TransportProtos.SessionInfoProto sessionInfo = session.getSessionInfo();
        if (sessionInfo != null) {
            transportService.recordActivity(sessionInfo);
        }
    }

    public TcpProtoTransportEntityService getProtoEntityService() {
        return protoEntityService;
    }
    public Collection<TcpDeviceSession> getClientSessions() {
        return clientSessions.values();
    }


    /**
     * SERVER 入站：若远端 IP 已绑定且配置文件为 {@link TcpWireAuthenticationMode#NONE}，则在 Core 侧静默校验访问令牌并注册会话。
     *
     * @return true 表示已走异步注册，此时须保持 autoRead=false 直至回调中打开
     */
    public boolean startServerWireAuth(ChannelHandlerContext ctx, TcpDeviceSession session) {
        var deviceIdOpt = tcpDedicatedListenPortService.findDeviceIdForDedicatedPortNoneSilentAuth(
                ctx.channel().localAddress(), ctx.channel().remoteAddress());
        if (deviceIdOpt.isEmpty()) {
            deviceIdOpt = tcpSourceBindingService.findDeviceIdForRemoteAddress(ctx.channel().remoteAddress());
        }
        if (deviceIdOpt.isEmpty()) {
            return false;
        }
        Device device = protoEntityService.getDeviceById(deviceIdOpt.get());
        if (device == null) {
            return false;
        }
        if (!sourceHostMatchesIfRequired(device, ctx.channel().remoteAddress())) {
            log.warn("[{}] TCP NONE: sourceHost mismatch, closing", device.getId());
            ctx.close();
            return true;
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration)) {
            return false;
        }
        TcpDeviceProfileTransportConfiguration ptc = (TcpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.NONE) {
            return false;
        }
        DeviceCredentials cred = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (cred.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            return false;
        }
        session.setDeviceProfile(profile);
        transportService.process(DeviceTransportType.TCP,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(cred.getCredentialsId()).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse response) {
                        if (!response.hasDeviceInfo()) {
                            log.warn("[{}] NONE wire auth: Core rejected credentials", device.getId());
                            ctx.close();
                            return;
                        }
                        ctx.channel().eventLoop().execute(() -> {
                            afterSuccessfulAuth(ctx, session, response);
                            ctx.channel().config().setAutoRead(true);
                        });
                    }
                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] NONE wire auth error", device.getId(), e);
                        ctx.close();
                    }
                });
        return true;
    }

    /**
     * SERVER {@link TcpWireAuthenticationMode#DEFERRED_PAYLOAD_TOKEN} / {@link TcpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID}：
     * 在 Core 会话注册前对每一帧按档案解析；本帧无身份字段则丢弃并等待；有字段则提交 Core 注册（TOKEN 模式字段值为 ACCESS_TOKEN；
     * DEVICE_ID 模式字段值为协议设备 ID，由监听端口 + 设备传输配置 {@code tcpWireAuthPayloadDeviceId} 定位 TB 设备后以该设备 ACCESS_TOKEN 注册）。
     */
    public void completeDeferredWireAuthServerAuth(ChannelHandlerContext ctx, TcpDeviceSession session, byte[] rawFrame) {
        DeviceProfile profile = session.getDeviceProfile();
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration ptc)) {
            log.warn("[{}] Deferred payload wire auth requires inbound session bound to device profile (use serverBindPort dedicated listen)",
                    session.getSessionId());
            session.endServerAuth();
            ctx.close();
            return;
        }
        TcpWireAuthenticationMode mode = ptc.getTcpWireAuthenticationMode();
        if (mode != TcpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN
                && mode != TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            log.warn("[{}] inbound handler expected deferred payload wire auth mode", session.getSessionId());
            session.endServerAuth();
            ctx.close();
            return;
        }
        Optional<String> fieldValueOpt = tcpMessageProcessor.extractDeferredWireAuthAccessToken(profile, session, rawFrame);
        if (fieldValueOpt.isEmpty() || StringUtils.isBlank(fieldValueOpt.get())) {
            log.debug("[{}] Deferred wire auth: identity field absent in this frame, waiting for next frame", session.getSessionId());
            return;
        }
        if (!session.tryBeginServerAuth()) {
            log.debug("[{}] Deferred wire auth: validation already in progress, skip this frame", session.getSessionId());
            return;
        }
        String fieldValue = fieldValueOpt.get().trim();
        if (mode == TcpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN) {
            submitDeferredAccessTokenValidation(ctx, session, rawFrame, fieldValue);
            return;
        }
        int localPort = ((InetSocketAddress) ctx.channel().localAddress()).getPort();
        Optional<DeviceId> deviceIdOpt = tcpDedicatedListenPortService.findDeviceIdByListenPortAndProtocolDeviceId(localPort, fieldValue);
        if (deviceIdOpt.isEmpty()) {
            log.warn("[{}] DEFERRED_PAYLOAD_DEVICE_ID: no TB device for local port {} and payload device id [{}]",
                    session.getSessionId(), localPort, fieldValue);
            session.endServerAuth();
            ctx.close();
            return;
        }
        Device device = protoEntityService.getDeviceById(deviceIdOpt.get());
        if (device == null) {
            log.warn("[{}] DEFERRED_PAYLOAD_DEVICE_ID: resolved device id {} not found", session.getSessionId(), deviceIdOpt.get());
            session.endServerAuth();
            ctx.close();
            return;
        }
        // 身份已由监听端口 + 负载协议设备 ID 确定，不再校验 sourceHost（NONE 多机同端口仍靠 IP 区分）。
        DeviceCredentials cred = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (cred.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            log.warn("[{}] DEFERRED_PAYLOAD_DEVICE_ID: device {} has no ACCESS_TOKEN credentials", session.getSessionId(), device.getId());
            session.endServerAuth();
            ctx.close();
            return;
        }
        submitDeferredAccessTokenValidation(ctx, session, rawFrame, cred.getCredentialsId());
    }

    private void submitDeferredAccessTokenValidation(ChannelHandlerContext ctx, TcpDeviceSession session, byte[] rawFrame, String accessToken) {
        transportService.process(DeviceTransportType.TCP,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(accessToken).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        if (!msg.hasDeviceInfo()) {
                            log.warn("[{}] Deferred wire auth: Core rejected credentials", session.getSessionId());
                            session.endServerAuth();
                            ctx.close();
                            return;
                        }
                        ctx.channel().eventLoop().execute(() -> {
                            session.setDeviceInfo(msg.getDeviceInfo());
                            session.setDeviceProfile(msg.getDeviceProfile());
                            session.setDeviceWireAuthenticated(true);
                            afterSuccessfulAuth(ctx, session, msg);
                            tcpMessageProcessor.replayDeferredUplinkAfterAuth(session, rawFrame);
                            recordUplinkFrameActivity(session);
                        });
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] Deferred wire auth: validate error", session.getSessionId(), e);
                        session.endServerAuth();
                        ctx.close();
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
                || !(device.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration)) {
            return true;
        }
        TcpDeviceTransportConfiguration dtc = (TcpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        Integer expectedPort = TcpEffectiveServerBindPort.resolve(profile, dtc);
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
        if (device.getDeviceData() == null || !(device.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration)) {
            return true;
        }
        TcpDeviceTransportConfiguration dtc = (TcpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
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
     * 专用监听端口（设备 {@code serverBindPort}）上入站时，从设备配置文件解析首段分帧与负载类型（与 {@link TcpDeviceProfileTransportConfiguration} 一致）。
     */
    public Optional<TcpInboundPipelineConfig> resolveInboundPipelineConfigForLocalPort(int localPort) {
        Optional<DeviceId> idOpt = tcpDedicatedListenPortService.findAnyDeviceIdForLocalPort(localPort);
        if (idOpt.isEmpty()) {
            return Optional.empty();
        }
        Device device = protoEntityService.getDeviceById(idOpt.get());
        if (device == null) {
            return Optional.empty();
        }
        DeviceProfile profile = deviceProfileCache.get(device.getDeviceProfileId());
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration)) {
            return Optional.empty();
        }
        TcpDeviceProfileTransportConfiguration tcp = (TcpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (tcp.getTcpTransportConnectMode() != TcpTransportConnectMode.SERVER) {
            return Optional.empty();
        }
        int fixed = tcpTransportService.getServerAuthFixedFrameLength();
        if (tcp.getTcpTransportFramingMode() == TcpTransportFramingMode.FIXED_LENGTH) {
            Integer n = tcp.getTcpFixedFrameLength();
            if (n != null && n > 0) {
                fixed = n;
            }
        } else {
            fixed = tcpTransportService.getServerAuthFixedFrameLength();
        }
        return Optional.of(new TcpInboundPipelineConfig(tcp.getTcpTransportFramingMode(), fixed, profile));
    }
}