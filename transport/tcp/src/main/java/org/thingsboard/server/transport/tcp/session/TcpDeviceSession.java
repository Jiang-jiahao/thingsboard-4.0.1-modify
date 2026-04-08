/**
 * Copyright © 2016-2025 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.tcp.session;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.adaptor.JsonConverter;
import io.netty.buffer.ByteBuf;
import com.google.gson.JsonElement;
import org.thingsboard.server.common.data.device.profile.TcpJsonWithoutMethodMode;
import org.thingsboard.server.common.data.device.profile.TcpTransportFramingMode;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.TransportTcpDataType;
import org.thingsboard.server.common.data.device.profile.HexTransportTcpDataConfiguration;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateTransportTcpDataConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.SessionMsgListener;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.common.transport.session.DeviceAwareSessionContext;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode;
import org.thingsboard.server.gen.transport.TransportProtos.AttributeUpdateNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetAttributeResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.SessionCloseNotificationProto;
import org.thingsboard.server.gen.transport.TransportProtos.ToDeviceRpcRequestMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToServerRpcResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToTransportUpdateCredentialsProto;
import org.thingsboard.server.transport.tcp.TcpTransportContext;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.Unpooled;
import org.thingsboard.server.transport.tcp.util.TcpPayloadUtil;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TcpDeviceSession extends DeviceAwareSessionContext implements SessionMsgListener {

    private final TcpTransportContext tcpTransportContext;
    private final TransportService transportService;
    private final AtomicInteger msgIdSeq = new AtomicInteger(0);
    @Getter
    @Setter
    private volatile Channel channel;
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


    /**
     * 入站 SERVER 连接在 Netty pipeline 首段实际使用的分帧（专用端口时等于设备配置文件，否则等于全局鉴权分帧）。
     */
    @Getter
    @Setter
    private volatile TcpTransportFramingMode inboundPipelineFramingMode;
    @Getter
    @Setter
    private volatile int inboundPipelineFixedFrameLength;

    public TcpDeviceSession(UUID sessionId, TcpTransportContext tcpTransportContext, boolean outboundClient) {
        super(sessionId);
        this.tcpTransportContext = tcpTransportContext;
        this.transportService = tcpTransportContext.getTransportService();
        this.outboundClient = outboundClient;
    }

    public TransportTcpDataType getPayloadDataType() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return TransportTcpDataType.JSON;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof TcpDeviceProfileTransportConfiguration) {
            TcpDeviceProfileTransportConfiguration tcpCfg = (TcpDeviceProfileTransportConfiguration) tc;
            return tcpCfg.getTransportTcpDataTypeConfiguration().getTransportTcpDataType();
        }
        return TransportTcpDataType.JSON;
    }

    /**
     * 当前 TCP 传输为 HEX 且已配置 {@link HexTransportTcpDataConfiguration} 时返回该配置，否则 {@code null}。
     */
    public HexTransportTcpDataConfiguration getHexTcpDataConfiguration() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return null;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof TcpDeviceProfileTransportConfiguration tcpCfg) {
            var dataCfg = tcpCfg.getTransportTcpDataTypeConfiguration();
            if (dataCfg instanceof HexTransportTcpDataConfiguration hexCfg) {
                return hexCfg;
            }
            if (dataCfg instanceof ProtocolTemplateTransportTcpDataConfiguration ptCfg) {
                return ptCfg.expandToHexTransportTcpDataConfiguration();
            }
        }
        return null;
    }

    public TcpTransportFramingMode getTcpTransportFramingMode() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return TcpTransportFramingMode.LINE;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof TcpDeviceProfileTransportConfiguration) {
            return ((TcpDeviceProfileTransportConfiguration) tc).getTcpTransportFramingMode();
        }
        return TcpTransportFramingMode.LINE;
    }

    /**
     * FIXED_LENGTH 分帧时从设备配置读取；未配置时返回 0（由调用方与全局默认处理）。
     */
    public int getTcpFixedFrameLengthForFraming() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return 0;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof TcpDeviceProfileTransportConfiguration) {
            Integer n = ((TcpDeviceProfileTransportConfiguration) tc).getTcpFixedFrameLength();
            return n != null ? n : 0;
        }
        return 0;
    }

    public void sendAuthFrame(String token) {
        int fixed = getTcpFixedFrameLengthForFraming();
        ByteBuf buf = TcpPayloadUtil.encodeAuthFrame(getTcpTransportFramingMode(), fixed, token);
        writeByteBuf(buf);
    }

    public void sendJsonPayload(JsonObject json) {
        int fixed = getTcpFixedFrameLengthForFraming();
        ByteBuf buf = TcpPayloadUtil.encodeBusinessFrame(getPayloadDataType(), getTcpTransportFramingMode(), fixed, json.toString());
        writeByteBuf(buf);
    }

    public void writeByteBuf(ByteBuf buf) {
        Channel ch = this.channel;
        if (ch != null && ch.isActive()) {
            ch.eventLoop().execute(() -> {
                if (ch.isActive()) {
                    ch.writeAndFlush(buf);
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
        Channel ch = this.channel;
        if (ch != null && ch.isActive()) {
            ch.close();
        }
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
    }

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, ToDeviceRpcRequestMsg rpcRequest) {
        JsonObject msg = new JsonObject();
        msg.addProperty("method", "rpc");
        msg.addProperty("requestId", rpcRequest.getRequestId());
        msg.addProperty("name", rpcRequest.getMethodName());
        msg.addProperty("params", rpcRequest.getParams());
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
        tcpTransportContext.onTcpSessionDeviceDeleted(this);
    }

    @Override
    public void onToTransportUpdateCredentials(ToTransportUpdateCredentialsProto toTransportUpdateCredentials) {
        log.info("[{}] Credentials update not supported over TCP in this version", getSessionId());
    }

    @Override
    public void onDeviceProfileUpdate(TransportProtos.SessionInfoProto newSessionInfo, DeviceProfile deviceProfile) {
        super.onDeviceProfileUpdate(newSessionInfo, deviceProfile);
        tcpTransportContext.onTcpDeviceProfileUpdated(this, deviceProfile);
    }

    @Override
    public void onDeviceUpdate(TransportProtos.SessionInfoProto sessionInfo, Device device, Optional<DeviceProfile> deviceProfileOpt) {
        super.onDeviceUpdate(sessionInfo, device, deviceProfileOpt);
        tcpTransportContext.onTcpDeviceUpdated(this, device, deviceProfileOpt);
    }

    public void processIncomingJsonLine(String jsonLine) {
        try {
            JsonElement el = JsonParser.parseString(jsonLine);
            if (el.isJsonObject()) {
                tcpTransportContext.getTcpMessageProcessor().processUplinkJson(this, el.getAsJsonObject());
            } else if (getTcpJsonWithoutMethodMode() == TcpJsonWithoutMethodMode.OPAQUE_FOR_RULE_ENGINE) {
                tcpTransportContext.getTcpMessageProcessor().processUplinkWithoutMethod(this, el);
            } else {
                log.warn("[{}] Expected JSON object line for TELEMETRY_FLAT mode", getSessionId());
            }
        } catch (Exception e) {
            log.warn("[{}] Failed to process TCP JSON line: {}", getSessionId(), jsonLine, e);
            transportService.errorEvent(getTenantId(), getDeviceId(), "tcpUplink", e);
        }
    }

    public boolean tryBeginServerAuth() {
        return serverAuthInFlight.compareAndSet(false, true);
    }

    public void endServerAuth() {
        serverAuthInFlight.set(false);
    }


    public TcpWireAuthenticationMode getTcpWireAuthenticationMode() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return TcpWireAuthenticationMode.TOKEN;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof TcpDeviceProfileTransportConfiguration) {
            return ((TcpDeviceProfileTransportConfiguration) tc).getTcpWireAuthenticationMode();
        }
        return TcpWireAuthenticationMode.TOKEN;
    }
    /**
     * CLIENT 建连后是否在链路上发送 {@code {"token":"..."}} 帧。
     */
    public boolean shouldSendWireAuthPayload() {
        return getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.NONE;
    }


    public TcpJsonWithoutMethodMode getTcpJsonWithoutMethodMode() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return TcpJsonWithoutMethodMode.TELEMETRY_FLAT;
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof TcpDeviceProfileTransportConfiguration) {
            return ((TcpDeviceProfileTransportConfiguration) tc).getTcpJsonWithoutMethodMode();
        }
        return TcpJsonWithoutMethodMode.TELEMETRY_FLAT;
    }
    public String getTcpOpaqueRuleEngineKey() {
        DeviceProfile profile = getDeviceProfile();
        if (profile == null || profile.getProfileData() == null || profile.getProfileData().getTransportConfiguration() == null) {
            return "tcpOpaquePayload";
        }
        var tc = profile.getProfileData().getTransportConfiguration();
        if (tc instanceof TcpDeviceProfileTransportConfiguration) {
            return ((TcpDeviceProfileTransportConfiguration) tc).getTcpOpaqueRuleEngineKey();
        }
        return "tcpOpaquePayload";
    }
}