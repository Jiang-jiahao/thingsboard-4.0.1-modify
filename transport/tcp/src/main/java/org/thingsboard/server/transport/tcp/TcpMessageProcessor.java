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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.device.profile.TcpJsonWithoutMethodMode;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.TransportTcpDataType;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.auth.ValidateDeviceCredentialsResponse;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.queue.util.TbTcpTransportComponent;
import org.thingsboard.server.transport.tcp.session.TcpDeviceSession;
import org.thingsboard.server.transport.tcp.util.TcpHexProtocolParser;
import org.thingsboard.server.transport.tcp.util.TcpPayloadUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
@TbTcpTransportComponent
@Component
@RequiredArgsConstructor
@Slf4j
public class TcpMessageProcessor {
    private final TransportService transportService;
    public void processUplinkJson(TcpDeviceSession session, JsonObject root) {
        if (!session.isCoreSessionReady()) {
            log.warn("[{}] Session not ready", session.getSessionId());
            return;
        }
        if (root.has("method")) {
            String method = root.get("method").getAsString();
            switch (method) {
                case "telemetry":
                    processTelemetry(session, root);
                    break;
                case "attributes":
                    processAttributes(session, root);
                    break;
                case "claim":
                    processClaim(session, root);
                    break;
                case "rpcResponse":
                    processRpcResponse(session, root);
                    break;
                case "toServerRpc":
                    processToServerRpc(session, root);
                    break;
                case "getAttributes":
                    processGetAttributes(session, root);
                    break;
                case "subscribeAttr":
                    processSubscribeAttr(session, root);
                    break;
                case "subscribeRpc":
                    processSubscribeRpc(session, root);
                    break;
                default:
                    log.warn("[{}] Unknown method {}", session.getSessionId(), method);
            }
        } else {
            processUplinkWithoutMethod(session, root);
        }
    }
    /**
     * 无 {@code method} 的上行：按设备配置文件 {@link TcpJsonWithoutMethodMode} 扁平写遥测或整包写入单键供规则引擎解析。
     */
    public void processUplinkWithoutMethod(TcpDeviceSession session, JsonElement payload) {
        if (!session.isCoreSessionReady()) {
            log.warn("[{}] Session not ready", session.getSessionId());
            return;
        }
        if (session.getPayloadDataType() == TransportTcpDataType.HEX
                || session.getPayloadDataType() == TransportTcpDataType.PROTOCOL_TEMPLATE) {
            var hexCfg = session.getHexTcpDataConfiguration();
            var parsedOpt = hexCfg != null
                    ? TcpHexProtocolParser.tryParseTelemetryFromHexPayload(
                    payload, hexCfg.getHexCommandProfiles(), hexCfg.getHexProtocolFields(),
                    hexCfg.getHexLtvRepeating(), hexCfg.getChecksum(),
                    session.getSessionId())
                    : Optional.<JsonObject>empty();
            if (parsedOpt.isPresent()) {
                transportService.process(session.getSessionInfo(), JsonConverter.convertToTelemetryProto(parsedOpt.get()),
                        TransportServiceCallback.EMPTY);
                return;
            }
        }
        if (session.getTcpJsonWithoutMethodMode() == TcpJsonWithoutMethodMode.OPAQUE_FOR_RULE_ENGINE) {
            JsonObject wrap = new JsonObject();
            wrap.addProperty(session.getTcpOpaqueRuleEngineKey(), opaquePayloadForRuleEngine(payload));
            transportService.process(session.getSessionInfo(), JsonConverter.convertToTelemetryProto(wrap),
                    TransportServiceCallback.EMPTY);
            return;
        }
        if (!payload.isJsonObject()) {
            log.warn("[{}] TELEMETRY_FLAT requires JSON object root, got {}", session.getSessionId(), payload.getClass().getSimpleName());
            return;
        }
        transportService.process(session.getSessionInfo(), JsonConverter.convertToTelemetryProto(payload),
                TransportServiceCallback.EMPTY);
    }
    /**
     * OPAQUE 模式下单键遥测的字符串内容：避免整段 JSON 再被 toString 套一层引号；
     * TCP HEX 上行若为 {@code {"hex":"..."}} 则只保留十六进制串。
     */
    private static String opaquePayloadForRuleEngine(JsonElement payload) {
        if (payload.isJsonPrimitive() && payload.getAsJsonPrimitive().isString()) {
            return payload.getAsString();
        }
        if (payload.isJsonObject()) {
            JsonObject o = payload.getAsJsonObject();
            if (o.size() == 1 && o.has(TcpPayloadUtil.TCP_HEX_FRAME_JSON_KEY)) {
                JsonElement hexEl = o.get(TcpPayloadUtil.TCP_HEX_FRAME_JSON_KEY);
                if (hexEl != null && hexEl.isJsonPrimitive() && hexEl.getAsJsonPrimitive().isString()) {
                    return hexEl.getAsString();
                }
            }
        }
        return payload.toString();
    }

    private void processTelemetry(TcpDeviceSession session, JsonObject root) {
        JsonElement body = root.get("body");
        if (body == null) {
            log.warn("[{}] telemetry without body", session.getSessionId());
            return;
        }
        transportService.process(session.getSessionInfo(), JsonConverter.convertToTelemetryProto(body),
                TransportServiceCallback.EMPTY);
    }
    private void processAttributes(TcpDeviceSession session, JsonObject root) {
        JsonElement body = root.get("body");
        if (body == null) {
            log.warn("[{}] attributes without body", session.getSessionId());
            return;
        }
        transportService.process(session.getSessionInfo(), JsonConverter.convertToAttributesProto(body),
                TransportServiceCallback.EMPTY);
    }
    private void processClaim(TcpDeviceSession session, JsonObject root) {
        JsonElement body = root.get("body");
        String json = body != null ? body.toString() : "{}";
        DeviceId deviceId = new DeviceId(new UUID(session.getSessionInfo().getDeviceIdMSB(), session.getSessionInfo().getDeviceIdLSB()));
        transportService.process(session.getSessionInfo(), JsonConverter.convertToClaimDeviceProto(deviceId, json),
                TransportServiceCallback.EMPTY);
    }
    private void processRpcResponse(TcpDeviceSession session, JsonObject root) {
        int requestId = root.get("requestId").getAsInt();
        String payload = root.has("payload") ? root.get("payload").toString() : "{}";
        transportService.process(session.getSessionInfo(),
                TransportProtos.ToDeviceRpcResponseMsg.newBuilder().setRequestId(requestId).setPayload(payload).build(),
                TransportServiceCallback.EMPTY);
    }
    private void processToServerRpc(TcpDeviceSession session, JsonObject root) {
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
    private void processGetAttributes(TcpDeviceSession session, JsonObject root) {
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
    private void processSubscribeAttr(TcpDeviceSession session, JsonObject root) {
        boolean unsubscribe = root.has("unsubscribe") && root.get("unsubscribe").getAsBoolean();
        transportService.process(session.getSessionInfo(),
                TransportProtos.SubscribeToAttributeUpdatesMsg.newBuilder()
                        .setUnsubscribe(unsubscribe)
                        .setSessionType(TransportProtos.SessionType.ASYNC)
                        .build(),
                TransportServiceCallback.EMPTY);
    }
    private void processSubscribeRpc(TcpDeviceSession session, JsonObject root) {
        boolean unsubscribe = root.has("unsubscribe") && root.get("unsubscribe").getAsBoolean();
        transportService.process(session.getSessionInfo(),
                TransportProtos.SubscribeToRPCMsg.newBuilder()
                        .setUnsubscribe(unsubscribe)
                        .setSessionType(TransportProtos.SessionType.ASYNC)
                        .build(),
                TransportServiceCallback.EMPTY);
    }
    public void processServerSideAuth(TcpDeviceSession session, JsonObject root, Consumer<ValidateDeviceCredentialsResponse> onSuccess) {
        if (!root.has("token")) {
            log.warn("[{}] Missing token in auth line", session.getSessionId());
            return;
        }
        String token = root.get("token").getAsString();
        transportService.process(DeviceTransportType.TCP,
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
}