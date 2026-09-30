package com.jnks.iot.server.transport.udp.session;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.adaptor.JsonConverter;
import io.netty.buffer.ByteBuf;
import com.google.gson.JsonElement;
import com.jnks.iot.server.common.data.device.profile.UdpJsonWithoutMethodMode;
import com.jnks.iot.server.common.data.device.profile.UdpTransportFramingMode;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.TransportUdpDataType;
import com.jnks.iot.server.common.data.device.profile.HexTransportUdpDataConfiguration;
import com.jnks.iot.server.common.data.device.profile.ProtocolTemplateTransportUdpDataConfiguration;
import com.jnks.iot.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.transport.SessionMsgListener;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import com.jnks.iot.server.common.transport.session.DeviceAwareSessionContext;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.common.data.device.profile.UdpWireAuthenticationMode;
import com.jnks.iot.server.gen.transport.TransportProtos.AttributeUpdateNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.GetAttributeResponseMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionCloseNotificationProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToDeviceRpcRequestMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToServerRpcResponseMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportUpdateCredentialsProto;
import com.jnks.iot.server.transport.udp.UdpTransportContext;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.Unpooled;
import com.jnks.iot.server.transport.udp.util.UdpPayloadUtil;
import com.jnks.iot.server.transport.udp.util.UdpRpcFrameEncoder;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class UdpDeviceSession extends DeviceAwareSessionContext implements SessionMsgListener {

    private final UdpTransportContext udpTransportContext;
    private final TransportService transportService;
    private final AtomicInteger msgIdSeq = new AtomicInteger(0);
    @Getter
    @Setter
    private volatile Channel channel;
    @Getter
    @Setter
    private volatile InetSocketAddress remoteAddress;
    /** 最近一次收到该设备数据报的时间；档案配了 udpReadIdleTimeoutSec 时由空闲清理任务据此关会话。 */
    @Getter
    @Setter
    private volatile long lastUplinkMs = System.currentTimeMillis();
    /**
     * 平台已向 Core 完成鉴权并注册会话（CLIENT 在出站 TCP 建连成功且 {@code channelActive} 中注册后为 true；SERVER 在收到首行 token 后为 true）。
     */
    @Getter
    @Setter
    private volatile boolean coreSessionReady;

    /**
     * 设备已通过链路上鉴权完成接入认证。
     */
    @Getter
    @Setter
    private volatile boolean deviceWireAuthenticated;


    private final AtomicBoolean serverAuthInFlight = new AtomicBoolean(false);
    private final AtomicLong serverAuthStartedAt = new AtomicLong(0);
    private final AtomicBoolean preAuthDropLogged = new AtomicBoolean(false);
    /**
     * 鉴权在途保护窗口：超过该时长视为上一次鉴权响应丢失（例如队列重平衡期间），允许设备重新发起；
     * 否则会话会永久卡在"鉴权在途"，后续帧全部被丢弃。测试中会调小该值。
     */
    private volatile long serverAuthTimeoutMs = 30_000L;


    /**
     * 入站 SERVER 连接在 Netty pipeline 首段实际使用的分帧（专用端口时等于设备配置文件，否则等于全局鉴权分帧）。
     */
    @Getter
    @Setter
    private volatile UdpTransportFramingMode inboundPipelineFramingMode;
    @Getter
    @Setter
    private volatile int inboundPipelineFixedFrameLength;

    public UdpDeviceSession(UUID sessionId, UdpTransportContext udpTransportContext) {
        super(sessionId);
        this.udpTransportContext = udpTransportContext;
        this.transportService = udpTransportContext.getTransportService();
    }

    public TransportUdpDataType getPayloadDataType() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return TransportUdpDataType.UTF8;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof UdpDeviceProfileTransportConfiguration) {
            UdpDeviceProfileTransportConfiguration tcpCfg = (UdpDeviceProfileTransportConfiguration) tc;
            return tcpCfg.getTransportUdpDataTypeConfiguration().getTransportUdpDataType();
        }
        return TransportUdpDataType.UTF8;
    }

    /**
     * 当前 TCP 传输为 HEX 且已配置 {@link HexTransportUdpDataConfiguration} 时返回该配置，否则 {@code null}。
     */
    public HexTransportUdpDataConfiguration getHexTcpDataConfiguration() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return null;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof UdpDeviceProfileTransportConfiguration tcpCfg) {
            var dataCfg = tcpCfg.getTransportUdpDataTypeConfiguration();
            if (dataCfg instanceof HexTransportUdpDataConfiguration hexCfg) {
                return hexCfg;
            }
            if (dataCfg instanceof ProtocolTemplateTransportUdpDataConfiguration ptCfg) {
                return ptCfg.expandToHexTransportUdpDataConfiguration();
            }
        }
        return null;
    }

    public UdpTransportFramingMode getUdpTransportFramingMode() {
        return UdpTransportFramingMode.NONE;
    }

    /**
     * FIXED_LENGTH 分帧时从设备配置读取；未配置时返回 0（由调用方与全局默认处理）。
     */
    public int getUdpFixedFrameLengthForFraming() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return 0;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof UdpDeviceProfileTransportConfiguration) {
            Integer n = ((UdpDeviceProfileTransportConfiguration) tc).getUdpFixedFrameLength();
            return n != null ? n : 0;
        }
        return 0;
    }

    public void sendJsonPayload(JsonObject json) {
        byte[] body = UdpPayloadUtil.bodyBytesForDataType(getPayloadDataType(), json.toString());
        ByteBuf buf = Unpooled.wrappedBuffer(body);
        writeByteBuf(buf);
    }

    public void writeByteBuf(ByteBuf buf) {
        Channel ch = this.channel;
        // 下行目的地：设备配置里填了下行地址就用它，没配就回发"设备最近上报的源地址"。
        InetSocketAddress remote = udpTransportContext.resolveDownlinkAddress(getDeviceId(), this.remoteAddress);
        if (remote == null) {
            log.warn("[{}] No UDP downlink address for device {}; dropping downlink frame", getSessionId(), getDeviceId());
            buf.release();
            return;
        }
        if (ch == null || !ch.isActive()) {
            buf.release();
            return;
        }
        ch.eventLoop().execute(() -> {
            if (ch.isActive()) {
                ch.writeAndFlush(new DatagramPacket(buf, remote));
            } else {
                buf.release();
            }
        });
    }

    public void writeRaw(String text) {
        writeByteBuf(Unpooled.wrappedBuffer(text.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public int nextMsgId() {
        return msgIdSeq.incrementAndGet();
    }

    public void close() {
        setConnected(false);
        udpTransportContext.evictInboundPeerSession(this);
    }

    @Override
    public void onGetAttributesResponse(GetAttributeResponseMsg getAttributesResponse) {
        JsonObject msg = new JsonObject();
        msg.addProperty("method", "getAttributesResponse");
        msg.add("data", JsonConverter.toJson(getAttributesResponse));
        sendJsonPayload(msg);
    }

    @Override
    public void onAttributeUpdate(UUID sessionId, AttributeUpdateNotificationMsg attributeUpdateNotification) {
        JsonObject msg = new JsonObject();
        msg.addProperty("method", "attributeUpdate");
        msg.add("data", JsonConverter.toJson(attributeUpdateNotification));
        sendJsonPayload(msg);
    }

    @Override
    public void onRemoteSessionCloseCommand(UUID sessionId, SessionCloseNotificationProto sessionCloseNotification) {
        JsonObject msg = new JsonObject();
        msg.addProperty("method", "sessionClose");
        msg.addProperty("reason", sessionCloseNotification.getReason().name());
        msg.addProperty("message", sessionCloseNotification.getMessage());
        sendJsonPayload(msg);
        // 走传输层的统一关闭：补 SESSION_CLOSED 与 STOPPED ——
        // Core 因非活跃超时 / 并发上限主动关会话走的就是这条路，只做本地清理的话
        // lc_event 里永远没有配对的 STOPPED。
        udpTransportContext.closeRegisteredInboundSession(this);
    }

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, ToDeviceRpcRequestMsg rpcRequest) {
        // 编码与"设备还没有会话时由出站会话直接发"共用同一条路径，见 UdpRpcFrameEncoder
        writeByteBuf(UdpRpcFrameEncoder.encode(getDeviceProfile(), rpcRequest));
    }

    @Override
    public void onToServerRpcResponse(ToServerRpcResponseMsg toServerResponse) {
        JsonObject msg = new JsonObject();
        msg.addProperty("method", "toServerRpcResponse");
        msg.addProperty("requestId", toServerResponse.getRequestId());
        msg.addProperty("payload", toServerResponse.getPayload());
        msg.addProperty("error", toServerResponse.getError());
        sendJsonPayload(msg);
    }

    @Override
    public void onDeviceDeleted(DeviceId deviceId) {
        udpTransportContext.onUdpSessionDeviceDeleted(this);
    }

    @Override
    public void onToTransportUpdateCredentials(ToTransportUpdateCredentialsProto toTransportUpdateCredentials) {
        log.info("[{}] Credentials update not supported over UDP in this version", getSessionId());
    }

    @Override
    public void onDeviceProfileUpdate(TransportProtos.SessionInfoProto newSessionInfo, DeviceProfile deviceProfile) {
        super.onDeviceProfileUpdate(newSessionInfo, deviceProfile);
        udpTransportContext.onUdpDeviceProfileUpdated(this, deviceProfile);
    }

    @Override
    public void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        super.onDeviceUpdate(sessionInfo, device, deviceProfileOpt);
        udpTransportContext.onUdpDeviceUpdated(this, device, deviceProfileOpt);
    }

    public void processIncomingJsonLine(String jsonLine) {
        try {
            JsonElement el = JsonParser.parseString(jsonLine);
            if (el.isJsonObject()) {
                udpTransportContext.getUdpMessageProcessor().processUplinkJson(this, el.getAsJsonObject());
            } else if (getPayloadDataType() == TransportUdpDataType.UTF8
                    || getPayloadDataType() == TransportUdpDataType.ASCII) {
                udpTransportContext.getUdpMessageProcessor().processUplinkWithoutMethod(this, el);
            } else {
                log.warn("[{}] Expected JSON object line for payload type {}", getSessionId(), getPayloadDataType());
            }
        } catch (Exception e) {
            TransportUdpDataType payloadType = getPayloadDataType();
            if (payloadType == TransportUdpDataType.UTF8
                    || payloadType == TransportUdpDataType.ASCII) {
                try {
                    JsonPrimitive fallback = new JsonPrimitive(jsonLine == null ? "" : jsonLine);
                    udpTransportContext.getUdpMessageProcessor().processUplinkWithoutMethod(this, fallback);
                    return;
                } catch (Exception inner) {
                    log.warn("[{}] Failed fallback non-JSON text processing: {}", getSessionId(), jsonLine, inner);
                }
            }
            log.warn("[{}] Failed to process TCP JSON line: {}", getSessionId(), jsonLine, e);
            transportService.errorEvent(getTenantId(), getDeviceId(), "udpUplink", e);
        }
    }

    public boolean tryBeginServerAuth() {
        long now = System.currentTimeMillis();
        long startedAt = serverAuthStartedAt.get();
        if (serverAuthInFlight.get()) {
            if (now - startedAt < serverAuthTimeoutMs) {
                return false;
            }
            log.warn("[{}] Server auth has been in flight for {} ms (timeout {} ms), allowing the device to retry",
                    getSessionId(), now - startedAt, serverAuthTimeoutMs);
        }
        serverAuthInFlight.set(true);
        serverAuthStartedAt.set(now);
        return true;
    }

    public void endServerAuth() {
        serverAuthInFlight.set(false);
        serverAuthStartedAt.set(0);
    }

    /**
     * 鉴权在途时被丢弃的帧只提示一次，避免同一设备反复重传时刷屏。
     */
    public boolean shouldLogPreAuthDrop() {
        return preAuthDropLogged.compareAndSet(false, true);
    }


    public UdpWireAuthenticationMode getUdpWireAuthenticationMode() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return UdpWireAuthenticationMode.NONE;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof UdpDeviceProfileTransportConfiguration) {
            return ((UdpDeviceProfileTransportConfiguration) tc).getUdpWireAuthenticationMode();
        }
        return UdpWireAuthenticationMode.NONE;
    }

    /**
     * SERVER：链路上鉴权为从业务负载解析协议设备号后再向 Core 注册。
     * 未绑定档案的会话必须返回 false —— 共享端口下鉴权前还不知道档案，那条路要留给延迟鉴权目录。
     */
    public boolean isDeferredPayloadWireAuth() {
        return getUdpWireAuthenticationMode() == UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
    }


    public UdpJsonWithoutMethodMode getUdpJsonWithoutMethodMode() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return UdpJsonWithoutMethodMode.TELEMETRY_FLAT;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof UdpDeviceProfileTransportConfiguration) {
            return ((UdpDeviceProfileTransportConfiguration) tc).getUdpJsonWithoutMethodMode();
        }
        return UdpJsonWithoutMethodMode.TELEMETRY_FLAT;
    }
    public String getUdpOpaqueRuleEngineKey() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return "tcpOpaquePayload";
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof UdpDeviceProfileTransportConfiguration) {
            return ((UdpDeviceProfileTransportConfiguration) tc).getUdpOpaqueRuleEngineKey();
        }
        return "tcpOpaquePayload";
    }
}