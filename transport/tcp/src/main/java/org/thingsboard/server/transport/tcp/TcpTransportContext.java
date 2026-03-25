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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.transport.tcp.netty.TcpPipelineBuilder;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
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
import org.thingsboard.server.common.transport.DeviceUpdatedEvent;
import org.thingsboard.server.common.transport.TransportDeviceProfileCache;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.auth.SessionInfoCreator;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.queue.util.TbTcpTransportComponent;
import org.thingsboard.server.transport.tcp.event.TcpTransportListChangedEvent;
import org.thingsboard.server.transport.tcp.session.TcpDeviceSession;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
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
    private final Collection<DeviceId> allTcpDeviceIds = new ConcurrentLinkedDeque<>();


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
        session.endServerAuth();
        ctx.channel().eventLoop().execute(() -> TcpPipelineBuilder.replaceFramingIfNeeded(ctx.pipeline(),
                session.getInboundPipelineFramingMode() != null ? session.getInboundPipelineFramingMode() : tcpTransportService.getServerAuthFramingMode(),
                session.getInboundPipelineFramingMode() != null ? session.getInboundPipelineFixedFrameLength() : tcpTransportService.getServerAuthFixedFrameLength(),
                session.getTcpTransportFramingMode(),
                session.getTcpFixedFrameLengthForFraming(),
                tcpTransportService.getMaxFrameLength()));
    }
    private void completeSessionRegistration(TcpDeviceSession session, ValidateDeviceCredentialsResponse msg) {
        TransportProtos.SessionInfoProto sessionInfo = SessionInfoCreator.create(msg, this, session.getSessionId());
        transportService.registerAsyncSession(sessionInfo, session);
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
        log.info("Establishing TCP CLIENT session for device {}", device.getId());
        DeviceProfile deviceProfile = deviceProfileCache.get(device.getDeviceProfileId());
        DeviceCredentials credentials = protoEntityService.getDeviceCredentialsByDeviceId(device.getId());
        if (credentials.getCredentialsType() != DeviceCredentialsType.ACCESS_TOKEN) {
            log.warn("[{}] Expected ACCESS_TOKEN credentials", device.getId());
            return;
        }
        TcpDeviceProfileTransportConfiguration profileCfg = (TcpDeviceProfileTransportConfiguration) deviceProfile.getProfileData().getTransportConfiguration();
        TcpDeviceTransportConfiguration deviceCfg = (TcpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        TcpDeviceSession session = new TcpDeviceSession(UUID.randomUUID(), this, true);
        session.setDeviceProfile(deviceProfile);
        transportService.process(DeviceTransportType.TCP,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(credentials.getCredentialsId()).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        if (msg.hasDeviceInfo()) {
                            completeSessionRegistration(session, msg);
                            openOutboundConnection(session, deviceCfg.getHost(), deviceCfg.getPort());
                            clientSessions.put(device.getId(), session);
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
    private void openOutboundConnection(TcpDeviceSession session, String host, int port) {
        Bootstrap b = new Bootstrap();
        b.group(tcpTransportService.getWorkerGroup())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
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
                log.error("[{}] Outbound TCP connect failed to {}:{}", session.getDeviceId(), host, port, future.cause());
                transportService.errorEvent(session.getTenantId(), session.getDeviceId(), "tcpClientConnect", future.cause());
            }
        });
    }
    public void onChannelClosed(TcpDeviceSession session) {
        if (session.getSessionInfo() != null) {
            transportService.deregisterSession(session.getSessionInfo());
        }
        if (session.getDeviceId() != null) {
            clientSessions.remove(session.getDeviceId());
            transportService.lifecycleEvent(session.getTenantId(), session.getDeviceId(), ComponentLifecycleEvent.STOPPED, true, null);
        }
        session.setConnected(false);
        session.endServerAuth();
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
        if (dtc.getServerBindPort() == null) {
            return true;
        }
        int localPort = ((InetSocketAddress) ctx.channel().localAddress()).getPort();
        if (localPort != dtc.getServerBindPort()) {
            log.warn("[{}] TCP auth rejected: expect listen port {} but socket local port is {}", deviceId, dtc.getServerBindPort(), localPort);
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