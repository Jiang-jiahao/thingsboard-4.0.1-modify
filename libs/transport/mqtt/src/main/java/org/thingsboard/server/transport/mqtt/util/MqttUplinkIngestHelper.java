/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.thingsboard.server.common.adaptor.AdaptorException;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.gen.transport.TransportProtos;

import java.nio.charset.StandardCharsets;

/**
 * MQTT 服务端上行负载转换，与 MQTT pull 的 wrap 遥测 / JSON 属性落点对齐。
 */
public final class MqttUplinkIngestHelper {

    private MqttUplinkIngestHelper() {
    }

    public static String payloadAsUtf8(MqttPublishMessage inbound) {
        if (inbound == null || inbound.payload() == null) {
            return "";
        }
        ByteBuf payload = inbound.payload();
        return payload.toString(StandardCharsets.UTF_8);
    }

    public static TransportProtos.PostTelemetryMsg toWrappedTelemetry(String jsonPayload, String telemetryKey) {
        String key = StringUtils.isNotBlank(telemetryKey) ? telemetryKey : "mqttPayload";
        JsonObject wrapper = new JsonObject();
        String body = jsonPayload != null ? jsonPayload : "";
        try {
            wrapper.add(key, JsonParser.parseString(body));
        } catch (Exception e) {
            wrapper.addProperty(key, body);
        }
        return JsonConverter.convertToTelemetryProto(wrapper);
    }

    public static TransportProtos.PostAttributeMsg toAttributes(String jsonPayload, boolean shared) throws AdaptorException {
        JsonElement parsed;
        try {
            parsed = JsonParser.parseString(jsonPayload != null ? jsonPayload : "");
        } catch (Exception e) {
            throw new AdaptorException("MQTT uplink attributes payload is not valid JSON", e);
        }
        if (!parsed.isJsonObject()) {
            throw new AdaptorException("MQTT uplink attributes payload is not a JSON object");
        }
        return JsonConverter.convertToAttributesProto(parsed).toBuilder().setShared(shared).build();
    }
}
