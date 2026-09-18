package org.thingsboard.server.transport.udp.session;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.adaptor.JsonConverter;
import io.netty.buffer.ByteBuf;
import com.google.gson.JsonElement;
import org.thingsboard.server.common.data.device.profile.UdpJsonWithoutMethodMode;
import org.thingsboard.server.common.data.device.profile.UdpTransportFramingMode;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.TransportUdpDataType;
import org.thingsboard.server.common.data.device.profile.HexTransportUdpDataConfiguration;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateTransportUdpDataConfiguration;
import org.thingsboard.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.SessionMsgListener;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.common.transport.session.DeviceAwareSessionContext;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode;
import org.thingsboard.server.gen.transport.TransportProtos.AttributeUpdateNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetAttributeResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SessionCloseNotificationProto;
import org.thingsboard.server.gen.transport.TransportProtos.ToDeviceRpcRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToServerRpcResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToTransportUpdateCredentialsProto;
import org.thingsboard.server.transport.udp.UdpTransportContext;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.Unpooled;
import org.thingsboard.server.transport.udp.util.UdpPayloadUtil;

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
    /**
     * 平台已向 Core 完成鉴权并注册会话（CLIENT 在出站 TCP 建连成功且 {@code channelActive} 中注册后为 true；SERVER 在收到首行 token 后为 true）。
     */
    @Getter
    @Setter
    private volatile boolean coreSessionReady;

    /**
     * CLIENT：令牌校验通过后暂存，在 Netty {@code channelActive} 时再向 Core 注册，避免未建连即显示在线。
     */
    private final AtomicReference<ValidateDeviceCredentialsResponse> pendingOutboundCredentials = new AtomicReference<>();

    public void stashPendingOutboundCredentials(ValidateDeviceCredentialsResponse msg) {
        pendingOutboundCredentials.set(msg);
    }

    public ValidateDeviceCredentialsResponse takePendingOutboundCredentials() {
        return pendingOutboundCredentials.getAndSet(null);
    }
    /**
     * SERVER 模式下设备已通过首行 token 完成接入认证。
     */
    @Getter
    @Setter
    private volatile boolean deviceWireAuthenticated;


    @Getter
    private final boolean outboundClient;

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

    public UdpDeviceSession(UUID sessionId, UdpTransportContext udpTransportContext, boolean outboundClient) {
        super(sessionId);
        this.udpTransportContext = udpTransportContext;
        this.transportService = udpTransportContext.getTransportService();
        this.outboundClient = outboundClient;
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
        InetSocketAddress remote = this.remoteAddress;
        if (ch != null && ch.isActive() && remote != null) {
            ch.eventLoop().execute(() -> {
                if (ch.isActive()) {
                    ch.writeAndFlush(new DatagramPacket(buf, remote));
                } else {
                    buf.release();
                }
            });
        } else {
            buf.release();
        }
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
        // Core 要求关闭会话时，主动断开 TCP 通道，确保 CLIENT 能按现有策略重连。
        close();
    }

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, ToDeviceRpcRequestMsg rpcRequest) {
        String params = rpcRequest.getParams();
        TransportUdpDataType dataType = getPayloadDataType();
        // 协议模板 / 原始字节：params.hex 已由 UI buildHex 组好，线上只发 decode 后的原始字节，不再包 RPC 信封 JSON。
        if ((dataType == TransportUdpDataType.RAW_BYTES || dataType == TransportUdpDataType.PROTOCOL_TEMPLATE)
                && UdpPayloadUtil.isHexTemplateRpcParams(params)) {
            ByteBuf buf = UdpPayloadUtil.encodeBusinessFrame(
                    dataType,
                    getUdpTransportFramingMode(),
                    getUdpFixedFrameLengthForFraming(),
                    params);
            writeByteBuf(buf);
            return;
        }
        JsonObject msg = new JsonObject();
        msg.addProperty("method", "rpc");
        msg.addProperty("requestId", rpcRequest.getRequestId());
        msg.addProperty("name", rpcRequest.getMethodName());
        msg.addProperty("params", params);
        msg.addProperty("expirationTime", rpcRequest.getExpirationTime());
        msg.addProperty("oneway", rpcRequest.getOneway());
        sendJsonPayload(msg);
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