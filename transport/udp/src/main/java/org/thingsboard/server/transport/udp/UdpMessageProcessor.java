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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.TransportUdpDataType;
import org.thingsboard.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateUplinkDataDestination;
import org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.TbUdpTransportComponent;
import org.thingsboard.server.transport.udp.session.UdpDeviceSession;
import org.thingsboard.server.transport.udp.util.UdpHexProtocolParser;
import org.thingsboard.server.transport.udp.util.UdpPayloadUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
@TbUdpTransportComponent
@Component
@RequiredArgsConstructor
@Slf4j
public class UdpMessageProcessor {
    private final TransportService transportService;
    public void processUplinkJson(UdpDeviceSession session, JsonObject root) {
        if (!session.isCoreSessionReady()) {
            log.warn("[{}] Session not ready", session.getSessionId());
            return;
        }
        JsonObject work = stripDeferredWireAuthTokenField(session, root);
        if (work.has("method")) {
            String method = work.get("method").getAsString();
            switch (method) {
                case "telemetry":
                    processTelemetry(session, work);
                    break;
                case "attributes":
                    processAttributes(session, work);
                    break;
                case "claim":
                    processClaim(session, work);
                    break;
                case "rpcResponse":
                    processRpcResponse(session, work);
                    break;
                case "toServerRpc":
                    processToServerRpc(session, work);
                    break;
                case "getAttributes":
                    processGetAttributes(session, work);
                    break;
                case "subscribeAttr":
                    processSubscribeAttr(session, work);
                    break;
                case "subscribeRpc":
                    processSubscribeRpc(session, work);
                    break;
                default:
                    log.warn("[{}] Unknown method {}", session.getSessionId(), method);
            }
        } else {
            processUplinkWithoutMethod(session, work);
        }
    }
    /**
     * 无 {@code method} 的上行：UTF-8（{@link TransportUdpDataType#UTF8}）/ ASCII 整帧写入单一可配置遥测键；
     * 原始字节（{@link TransportUdpDataType#RAW_BYTES}）/ 协议模板仅走 {@link UdpHexProtocolParser}，解析失败则丢弃。
     */
    public void processUplinkWithoutMethod(UdpDeviceSession session, JsonElement payload) {
        if (!session.isCoreSessionReady()) {
            log.warn("[{}] Session not ready", session.getSessionId());
            return;
        }
        TransportUdpDataType payloadType = session.getPayloadDataType();
        if (payloadType == TransportUdpDataType.RAW_BYTES
                || payloadType == TransportUdpDataType.PROTOCOL_TEMPLATE) {
            var hexCfg = session.getHexTcpDataConfiguration();
            if (hexCfg == null) {
                log.warn("[{}] HEX/PROTOCOL_TEMPLATE uplink but profile has no HEX/protocol-template configuration",
                        session.getSessionId());
                return;
            }
            var parsedOpt = UdpHexProtocolParser.tryParseUplinkPayloadFromHex(
                    payload, hexCfg.getHexCommandProfiles(), hexCfg.getHexProtocolFields(),
                    hexCfg.getHexLtvRepeating(), hexCfg.getChecksum(),
                    session.getSessionId());
            if (parsedOpt.isEmpty()) {
                log.warn("[{}] HEX/PROTOCOL_TEMPLATE frame did not match parser rules (no telemetry emitted)",
                        session.getSessionId());
                return;
            }
            var parsed = parsedOpt.get();
            JsonObject payloadForCore = stripDeferredWireAuthTokenField(session, parsed.getPayload());
            String matchedProfile = payloadForCore.has("hexCmdProfile")
                    ? payloadForCore.get("hexCmdProfile").getAsString()
                    : "<default-template-fallback>";
            log.info("[{}] HEX uplink route decision: profile={}, destination={}",
                    session.getSessionId(), matchedProfile, parsed.getDestination());
            if (parsed.getDestination() == ProtocolTemplateUplinkDataDestination.ATTRIBUTES) {
                transportService.process(session.getSessionInfo(), JsonConverter.convertToAttributesProto(payloadForCore),
                        TransportServiceCallback.EMPTY);
            } else {
                transportService.process(session.getSessionInfo(), JsonConverter.convertToTelemetryProto(payloadForCore),
                        TransportServiceCallback.EMPTY);
            }
            return;
        }
        if (payloadType == TransportUdpDataType.UTF8
                || payloadType == TransportUdpDataType.ASCII) {
            String telemetryKey = session.getUdpOpaqueRuleEngineKey();
            if (telemetryKey == null || telemetryKey.isBlank()) {
                telemetryKey = "tcpOpaquePayload";
            }
            JsonElement inner = payload;
            if (payload.isJsonObject()) {
                inner = stripDeferredWireAuthTokenField(session, payload.getAsJsonObject());
            }
            JsonObject wrap = new JsonObject();
            wrap.add(telemetryKey, inner);
            transportService.process(session.getSessionInfo(), JsonConverter.convertToTelemetryProto(wrap),
                    TransportServiceCallback.EMPTY);
            return;
        }
        log.warn("[{}] Uplink without method not supported for payload type {}", session.getSessionId(), payloadType);
    }

    private void processTelemetry(UdpDeviceSession session, JsonObject root) {
        JsonElement body = root.get("body");
        if (body == null) {
            log.warn("[{}] telemetry without body", session.getSessionId());
            return;
        }
        transportService.process(session.getSessionInfo(), JsonConverter.convertToTelemetryProto(body),
                TransportServiceCallback.EMPTY);
    }
    private void processAttributes(UdpDeviceSession session, JsonObject root) {
        JsonElement body = root.get("body");
        if (body == null) {
            log.warn("[{}] attributes without body", session.getSessionId());
            return;
        }
        transportService.process(session.getSessionInfo(), JsonConverter.convertToAttributesProto(body),
                TransportServiceCallback.EMPTY);
    }
    private void processClaim(UdpDeviceSession session, JsonObject root) {
        JsonElement body = root.get("body");
        String json = body != null ? body.toString() : "{}";
        DeviceId deviceId = new DeviceId(new UUID(session.getSessionInfo().getDeviceIdMSB(), session.getSessionInfo().getDeviceIdLSB()));
        transportService.process(session.getSessionInfo(), JsonConverter.convertToClaimDeviceProto(deviceId, json),
                TransportServiceCallback.EMPTY);
    }
    private void processRpcResponse(UdpDeviceSession session, JsonObject root) {
        int requestId = root.get("requestId").getAsInt();
        String payload = root.has("payload") ? root.get("payload").toString() : "{}";
        transportService.process(session.getSessionInfo(),
                TransportProtos.ToDeviceRpcResponseMsg.newBuilder().setRequestId(requestId).setPayload(payload).build(),
                TransportServiceCallback.EMPTY);
    }
    private void processToServerRpc(UdpDeviceSession session, JsonObject root) {
        JsonElement body = root.get("body");
        if (body == null) {
            log.warn("[{}] toServerRpc without body", session.getSessionId());
            return;
        }
        int requestId = session.nextMsgId();
        transportService.process(session.getSessionInfo(),
                JsonConverter.convertToServerRpcRequest(body, requestId),
                TransportServiceCallback.EMPTY);
    }
    private void processGetAttributes(UdpDeviceSession session, JsonObject root) {
        TransportProtos.GetAttributeRequestMsg.Builder b = TransportProtos.GetAttributeRequestMsg.newBuilder()
                .setRequestId(session.nextMsgId());
        if (root.has("clientKeys")) {
            b.addAllClientAttributeNames(splitKeys(root.get("clientKeys")));
        }
        if (root.has("sharedKeys")) {
            b.addAllSharedAttributeNames(splitKeys(root.get("sharedKeys")));
        }
        transportService.process(session.getSessionInfo(), b.build(), TransportServiceCallback.EMPTY);
    }
    private List<String> splitKeys(JsonElement keysEl) {
        List<String> keys = new ArrayList<>();
        if (keysEl == null || keysEl.isJsonNull()) {
            return keys;
        }
        if (keysEl.isJsonPrimitive()) {
            String s = keysEl.getAsString();
            if (!s.isBlank()) {
                for (String p : s.split(",")) {
                    String t = p.trim();
                    if (!t.isEmpty()) {
                        keys.add(t);
                    }
                }
            }
            return keys;
        }
        if (keysEl.isJsonArray()) {
            JsonArray arr = keysEl.getAsJsonArray();
            for (JsonElement e : arr) {
                if (e.isJsonPrimitive()) {
                    keys.add(e.getAsString());
                }
            }
        }
        return keys;
    }
    private void processSubscribeAttr(UdpDeviceSession session, JsonObject root) {
        boolean unsubscribe = root.has("unsubscribe") && root.get("unsubscribe").getAsBoolean();
        transportService.process(session.getSessionInfo(),
                TransportProtos.SubscribeToAttributeUpdatesMsg.newBuilder()
                        .setUnsubscribe(unsubscribe)
                        .setSessionType(TransportProtos.SessionType.ASYNC)
                        .build(),
                TransportServiceCallback.EMPTY);
    }
    private void processSubscribeRpc(UdpDeviceSession session, JsonObject root) {
        boolean unsubscribe = root.has("unsubscribe") && root.get("unsubscribe").getAsBoolean();
        transportService.process(session.getSessionInfo(),
                TransportProtos.SubscribeToRPCMsg.newBuilder()
                        .setUnsubscribe(unsubscribe)
                        .setSessionType(TransportProtos.SessionType.ASYNC)
                        .build(),
                TransportServiceCallback.EMPTY);
    }

    /**
     * DEFERRED 模式且会话已就绪时，从 JSON 对象副本中移除档案配置的身份字段，避免写入遥测/属性。
     */
    private JsonObject stripDeferredWireAuthTokenField(UdpDeviceSession session, JsonObject root) {
        UdpWireAuthenticationMode mode = session.getUdpWireAuthenticationMode();
        if (mode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN
                && mode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return root;
        }
        Optional<String> keyOpt = deferredWireAuthPayloadIdentityJsonKey(session.getDeviceProfile());
        if (keyOpt.isEmpty() || !root.has(keyOpt.get())) {
            return root;
        }
        JsonObject copy = root.deepCopy();
        copy.remove(keyOpt.get());
        return copy;
    }

    private static Optional<String> deferredWireAuthPayloadIdentityJsonKey(DeviceProfile profile) {
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration ptc)) {
            return Optional.empty();
        }
        UdpWireAuthenticationMode mode = ptc.getUdpWireAuthenticationMode();
        if (mode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN
                && mode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return Optional.empty();
        }
        String k = ptc.getUdpDeferredWireAuthTokenJsonKey();
        if (StringUtils.isBlank(k)) {
            return Optional.empty();
        }
        return Optional.of(k.trim());
    }

    public void processServerSideAuth(UdpDeviceSession session, JsonObject root, Consumer<ValidateDeviceCredentialsResponse> onSuccess) {
        if (!root.has("token")) {
            log.warn("[{}] Missing token in auth line", session.getSessionId());
            session.endServerAuth();
            return;
        }
        String token = root.get("token").getAsString();
        transportService.process(DeviceTransportType.UDP,
                TransportProtos.ValidateDeviceTokenRequestMsg.newBuilder().setToken(token).build(),
                new TransportServiceCallback<>() {
                    @Override
                    public void onSuccess(ValidateDeviceCredentialsResponse msg) {
                        if (msg.hasDeviceInfo()) {
                            session.setDeviceInfo(msg.getDeviceInfo());
                            session.setDeviceProfile(msg.getDeviceProfile());
                            session.setDeviceWireAuthenticated(true);
                            onSuccess.accept(msg);
                        } else {
                            log.warn("[{}] Auth failed", session.getSessionId());
                            session.endServerAuth();
                            session.close();
                        }
                    }
                    @Override
                    public void onError(Throwable e) {
                        log.warn("[{}] Auth error", session.getSessionId(), e);
                        session.endServerAuth();
                        session.close();
                    }
                });
    }

    /**
     * SERVER 延迟链路上鉴权：从<strong>当前帧</strong>按业务类型解码后的 JSON 中取档案配置的字段字符串
     * （{@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_TOKEN} 为 ACCESS_TOKEN；
     * {@link UdpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 为协议设备 ID）。未注册前可多次尝试，无字段的帧忽略。
     */
    public Optional<String> extractDeferredWireAuthAccessToken(DeviceProfile profile, UdpDeviceSession session, byte[] rawFrame) {
        if (profile == null || rawFrame == null) {
            return Optional.empty();
        }
        if (profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration ptc)) {
            return Optional.empty();
        }
        String key = ptc.getUdpDeferredWireAuthTokenJsonKey();
        if (StringUtils.isBlank(key)) {
            return Optional.empty();
        }
        TransportUdpDataType type = session.getPayloadDataType();
        String json;
        try {
            json = UdpPayloadUtil.decodePayloadBytes(type, rawFrame);
        } catch (Exception e) {
            log.debug("[{}] deferred wire auth: decode payload failed: {}", session.getSessionId(), e.getMessage());
            return Optional.empty();
        }
        if (StringUtils.isBlank(json)) {
            return Optional.empty();
        }
        final JsonElement root;
        try {
            root = JsonParser.parseString(json);
        } catch (Exception e) {
            log.warn("[{}] deferred wire auth: JSON parse failed", session.getSessionId(), e);
            return Optional.empty();
        }
        if (type == TransportUdpDataType.UTF8 || type == TransportUdpDataType.ASCII) {
            if (root.isJsonObject()) {
                return tokenStringFromJsonObject(root.getAsJsonObject(), key);
            }
            return Optional.empty();
        }
        if (type == TransportUdpDataType.RAW_BYTES || type == TransportUdpDataType.PROTOCOL_TEMPLATE) {
            var hexCfg = session.getHexTcpDataConfiguration();
            if (hexCfg == null) {
                return Optional.empty();
            }
            return UdpHexProtocolParser.tryParseUplinkPayloadFromHex(
                    root, hexCfg.getHexCommandProfiles(), hexCfg.getHexProtocolFields(),
                    hexCfg.getHexLtvRepeating(), hexCfg.getChecksum(),
                    session.getSessionId())
                    .flatMap(parsed -> tokenStringFromJsonObject(parsed.getPayload(), key));
        }
        return Optional.empty();
    }

    private static Optional<String> tokenStringFromJsonObject(JsonObject o, String key) {
        if (!o.has(key)) {
            return Optional.empty();
        }
        JsonElement el = o.get(key);
        if (el == null || el.isJsonNull()) {
            return Optional.empty();
        }
        if (el.isJsonPrimitive()) {
            return Optional.of(el.getAsString());
        }
        return Optional.empty();
    }

    /**
     * 鉴权成功后重放<strong>触发鉴权的那一帧</strong>上行：从 JSON 副本中移除令牌字段，再按原逻辑入库。
     */
    public void replayDeferredUplinkAfterAuth(UdpDeviceSession session, byte[] rawFrame) {
        if (!session.isCoreSessionReady() || rawFrame == null) {
            return;
        }
        DeviceProfile profile = session.getDeviceProfile();
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration ptc)) {
            return;
        }
        UdpWireAuthenticationMode mode = ptc.getUdpWireAuthenticationMode();
        if (mode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN
                && mode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return;
        }
        String key = ptc.getUdpDeferredWireAuthTokenJsonKey();
        if (StringUtils.isBlank(key)) {
            return;
        }
        TransportUdpDataType type = session.getPayloadDataType();
        String json = UdpPayloadUtil.decodePayloadBytes(type, rawFrame);
        if (StringUtils.isBlank(json)) {
            return;
        }
        JsonElement root;
        try {
            root = JsonParser.parseString(json);
        } catch (Exception e) {
            log.warn("[{}] deferred replay: JSON parse failed", session.getSessionId(), e);
            return;
        }
        if (type == TransportUdpDataType.UTF8 || type == TransportUdpDataType.ASCII) {
            if (!root.isJsonObject()) {
                return;
            }
            JsonObject o = root.getAsJsonObject().deepCopy();
            o.remove(key);
            if (o.size() == 0) {
                return;
            }
            if (o.has("method")) {
                processUplinkJson(session, o);
            } else {
                processUplinkWithoutMethod(session, o);
            }
            return;
        }
        if (type == TransportUdpDataType.RAW_BYTES || type == TransportUdpDataType.PROTOCOL_TEMPLATE) {
            var hexCfg = session.getHexTcpDataConfiguration();
            if (hexCfg == null) {
                return;
            }
            var parsedOpt = UdpHexProtocolParser.tryParseUplinkPayloadFromHex(
                    root, hexCfg.getHexCommandProfiles(), hexCfg.getHexProtocolFields(),
                    hexCfg.getHexLtvRepeating(), hexCfg.getChecksum(),
                    session.getSessionId());
            if (parsedOpt.isEmpty()) {
                return;
            }
            var parsed = parsedOpt.get();
            JsonObject body = parsed.getPayload().deepCopy();
            body.remove(key);
            if (body.size() == 0) {
                return;
            }
            emitParsedHexUplink(session, new UdpHexProtocolParser.ParsedUplinkPayload(body, parsed.getDestination()));
        }
    }

    private void emitParsedHexUplink(UdpDeviceSession session, UdpHexProtocolParser.ParsedUplinkPayload parsed) {
        if (parsed.getDestination() == ProtocolTemplateUplinkDataDestination.ATTRIBUTES) {
            transportService.process(session.getSessionInfo(), JsonConverter.convertToAttributesProto(parsed.getPayload()),
                    TransportServiceCallback.EMPTY);
        } else {
            transportService.process(session.getSessionInfo(), JsonConverter.convertToTelemetryProto(parsed.getPayload()),
                    TransportServiceCallback.EMPTY);
        }
    }
}