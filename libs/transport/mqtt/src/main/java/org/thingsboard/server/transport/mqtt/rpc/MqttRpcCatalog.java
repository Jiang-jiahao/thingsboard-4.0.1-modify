/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.rpc;

import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;
import org.thingsboard.server.common.transport.auth.TransportDeviceInfo;

import java.util.List;

public final class MqttRpcCatalog {

    private MqttRpcCatalog() {
    }

    public static DeviceProfileRpcMethod find(DeviceProfile profile, String methodName) {
        if (StringUtils.isBlank(methodName) || profile == null || profile.getProfileData() == null) {
            return null;
        }
        List<DeviceProfileRpcMethod> methods = profile.getProfileData().getRpcMethods();
        if (methods == null || methods.isEmpty()) {
            return null;
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m != null && methodName.equals(m.getId())) {
                return m;
            }
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (m != null && methodName.equals(m.getDeviceMethod())) {
                return m;
            }
        }
        return null;
    }

    public static boolean isMqttRpcBinding(DeviceProfileRpcMethod method) {
        if (method == null || method.getBindingType() == null) {
            return false;
        }
        return method.getBindingType() == DeviceProfileRpcBindingType.NATIVE
                || method.getBindingType() == DeviceProfileRpcBindingType.MQTT_CUSTOM;
    }

    public static boolean isCustomRequestSubscribeTopic(DeviceProfile profile, Device device,
                                                        TransportDeviceInfo deviceInfo, String topic) {
        if (StringUtils.isBlank(topic) || profile == null || profile.getProfileData() == null) {
            return false;
        }
        List<DeviceProfileRpcMethod> methods = profile.getProfileData().getRpcMethods();
        if (methods == null || methods.isEmpty()) {
            return false;
        }
        for (DeviceProfileRpcMethod m : methods) {
            if (!isMqttRpcBinding(m) || StringUtils.isBlank(m.getMqttRequestTopic())) {
                continue;
            }
            String filter = MqttRpcTemplateResolver.toRequestSubscribeFilter(m.getMqttRequestTopic(), device, deviceInfo);
            if (MqttRpcCommandFactory.topicMatches(filter, topic)
                    || topicEqualsOrCovers(topic, filter)) {
                return true;
            }
        }
        return false;
    }

    private static boolean topicEqualsOrCovers(String subscription, String filter) {
        if (subscription == null || filter == null) {
            return false;
        }
        if (subscription.equals(filter)) {
            return true;
        }
        if (subscription.endsWith("/#")) {
            String prefix = subscription.substring(0, subscription.length() - 2);
            return filter.equals(prefix) || filter.startsWith(prefix + "/");
        }
        if (subscription.contains("+")) {
            return MqttRpcCommandFactory.topicMatches(subscription, filter);
        }
        return false;
    }
}
