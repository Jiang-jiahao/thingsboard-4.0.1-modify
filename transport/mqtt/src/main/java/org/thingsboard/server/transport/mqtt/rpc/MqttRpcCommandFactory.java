/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.rpc;

import lombok.Builder;
import lombok.Value;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.adaptor.JsonConverter;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.transport.auth.TransportDeviceInfo;
import org.thingsboard.server.gen.transport.TransportProtos;

/**
 * 解析 MQTT RPC 实际发布主题、载荷与 QoS。
 */
public final class MqttRpcCommandFactory {

    private MqttRpcCommandFactory() {
    }

    @Value
    @Builder
    public static class Command {
        boolean useStandardNativeTopic;
        String requestTopic;
        String responseTopic;
        String payload;
        int qos;
        TransportProtos.ToDeviceRpcRequestMsg requestToDeliver;
    }

    public static Command resolve(DeviceProfileRpcMethod method,
                                  TransportProtos.ToDeviceRpcRequestMsg request,
                                  Device device,
                                  TransportDeviceInfo deviceInfo,
                                  boolean mqttPull) {
        TransportProtos.ToDeviceRpcRequestMsg toDeliver = rewriteNativeMethod(method, request);
        int qos = resolveQos(method);
        if (method == null) {
            if (mqttPull) {
                throw new IllegalArgumentException("MQTT pull RPC method not found: " + request.getMethodName());
            }
            return Command.builder()
                    .useStandardNativeTopic(true)
                    .qos(qos)
                    .requestToDeliver(toDeliver)
                    .payload(JsonConverter.toJson(toDeliver, false).toString())
                    .build();
        }
        if (method.getBindingType() == DeviceProfileRpcBindingType.MQTT_CUSTOM) {
            return resolveCustom(method, toDeliver, device, deviceInfo, qos);
        }
        if (method.getBindingType() == DeviceProfileRpcBindingType.NATIVE) {
            return resolveNative(method, toDeliver, device, deviceInfo, qos, mqttPull);
        }
        if (mqttPull) {
            throw new IllegalArgumentException("Unsupported MQTT RPC binding: " + method.getBindingType());
        }
        return Command.builder()
                .useStandardNativeTopic(true)
                .qos(qos)
                .requestToDeliver(toDeliver)
                .payload(JsonConverter.toJson(toDeliver, false).toString())
                .build();
    }

    private static Command resolveNative(DeviceProfileRpcMethod method,
                                         TransportProtos.ToDeviceRpcRequestMsg toDeliver,
                                         Device device,
                                         TransportDeviceInfo deviceInfo,
                                         int qos,
                                         boolean mqttPull) {
        String payload = JsonConverter.toJson(toDeliver, false).toString();
        if (StringUtils.isBlank(method.getMqttRequestTopic())) {
            if (mqttPull) {
                throw new IllegalArgumentException("NATIVE MQTT pull RPC requires mqttRequestTopic: " + method.getId());
            }
            return Command.builder()
                    .useStandardNativeTopic(true)
                    .qos(qos)
                    .requestToDeliver(toDeliver)
                    .payload(payload)
                    .build();
        }
        String requestTopic = MqttRpcTemplateResolver.resolve(
                method.getMqttRequestTopic(), device, deviceInfo, toDeliver.getParams(),
                toDeliver.getRequestId(), toDeliver.getMethodName());
        String responseTopic = resolveResponseTopic(method, toDeliver, device, deviceInfo);
        return Command.builder()
                .useStandardNativeTopic(false)
                .requestTopic(requestTopic)
                .responseTopic(responseTopic)
                .payload(payload)
                .qos(qos)
                .requestToDeliver(toDeliver)
                .build();
    }

    private static Command resolveCustom(DeviceProfileRpcMethod method,
                                         TransportProtos.ToDeviceRpcRequestMsg toDeliver,
                                         Device device,
                                         TransportDeviceInfo deviceInfo,
                                         int qos) {
        if (StringUtils.isBlank(method.getMqttRequestTopic())) {
            throw new IllegalArgumentException("MQTT_CUSTOM RPC requires mqttRequestTopic: " + method.getId());
        }
        String requestTopic = MqttRpcTemplateResolver.resolve(
                method.getMqttRequestTopic(), device, deviceInfo, toDeliver.getParams(),
                toDeliver.getRequestId(), method.getId());
        String payload = MqttRpcTemplateResolver.resolve(
                method.getMqttPayloadTemplate(), device, deviceInfo, toDeliver.getParams(),
                toDeliver.getRequestId(), method.getId());
        if (payload == null) {
            payload = toDeliver.getParams();
        }
        String responseTopic = resolveResponseTopic(method, toDeliver, device, deviceInfo);
        return Command.builder()
                .useStandardNativeTopic(false)
                .requestTopic(requestTopic)
                .responseTopic(responseTopic)
                .payload(payload)
                .qos(qos)
                .requestToDeliver(toDeliver)
                .build();
    }

    private static String resolveResponseTopic(DeviceProfileRpcMethod method,
                                               TransportProtos.ToDeviceRpcRequestMsg toDeliver,
                                               Device device,
                                               TransportDeviceInfo deviceInfo) {
        if (toDeliver.getOneway() || StringUtils.isBlank(method.getMqttResponseTopic())) {
            return null;
        }
        return MqttRpcTemplateResolver.resolve(
                method.getMqttResponseTopic(), device, deviceInfo, toDeliver.getParams(),
                toDeliver.getRequestId(),
                StringUtils.isNotBlank(toDeliver.getMethodName()) ? toDeliver.getMethodName() : method.getId());
    }

    public static TransportProtos.ToDeviceRpcRequestMsg rewriteNativeMethod(DeviceProfileRpcMethod method,
                                                                            TransportProtos.ToDeviceRpcRequestMsg request) {
        if (method == null || method.getBindingType() != DeviceProfileRpcBindingType.NATIVE
                || StringUtils.isBlank(method.getDeviceMethod())
                || method.getDeviceMethod().equals(request.getMethodName())) {
            return request;
        }
        return request.toBuilder().setMethodName(method.getDeviceMethod()).build();
    }

    public static int resolveQos(DeviceProfileRpcMethod method) {
        Integer qos = method != null ? method.getMqttQos() : null;
        if (qos == null) {
            return 1;
        }
        if (qos < 0) {
            return 0;
        }
        if (qos > 2) {
            return 2;
        }
        return qos;
    }

    public static String normalizeResponsePayload(String body) {
        if (StringUtils.isBlank(body)) {
            return "{}";
        }
        try {
            JacksonUtil.toJsonNode(body);
            return body;
        } catch (IllegalArgumentException ignored) {
            return JacksonUtil.toString(JacksonUtil.newObjectNode().put("response", body));
        }
    }

    public static Integer tryExtractRequestId(String payload) {
        if (StringUtils.isBlank(payload)) {
            return null;
        }
        try {
            var node = JacksonUtil.toJsonNode(payload);
            if (node == null || !node.isObject()) {
                return null;
            }
            if (node.has("requestId") && node.get("requestId").canConvertToInt()) {
                return node.get("requestId").asInt();
            }
            if (node.has("id") && node.get("id").canConvertToInt()) {
                return node.get("id").asInt();
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    public static boolean topicMatches(String filter, String topic) {
        if (filter == null || topic == null) {
            return false;
        }
        if (filter.equals(topic)) {
            return true;
        }
        if (filter.endsWith("/#")) {
            String prefix = filter.substring(0, filter.length() - 2);
            return topic.equals(prefix) || topic.startsWith(prefix + "/");
        }
        if (filter.contains("+")) {
            String[] filterParts = filter.split("/", -1);
            String[] topicParts = topic.split("/", -1);
            if (filterParts.length != topicParts.length) {
                return false;
            }
            for (int i = 0; i < filterParts.length; i++) {
                if (!"+".equals(filterParts[i]) && !filterParts[i].equals(topicParts[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
