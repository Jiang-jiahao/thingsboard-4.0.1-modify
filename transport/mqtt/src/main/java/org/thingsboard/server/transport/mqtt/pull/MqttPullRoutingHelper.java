/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;

import java.util.List;

final class MqttPullRoutingHelper {

    private MqttPullRoutingHelper() {
    }

    static boolean shouldRouteToMultipleDevices(HttpPullDeviceRoutingConfiguration routing, String body) {
        if (routing == null) {
            return false;
        }
        HttpPullRoutingMode mode = routing.getRoutingMode() != null
                ? routing.getRoutingMode() : HttpPullRoutingMode.SINGLE_DEVICE;
        if (mode == HttpPullRoutingMode.SINGLE_DEVICE) {
            return false;
        }
        if (mode == HttpPullRoutingMode.PER_MESSAGE) {
            return StringUtils.isNotBlank(routing.getDeviceIdJsonPath())
                    || routing.getDeviceIdTopicSegmentIndex() != null;
        }
        if (mode == HttpPullRoutingMode.MULTI_DEVICE) {
            return StringUtils.isNotBlank(routing.getDeviceIdJsonPath());
        }
        List<Object> elements = MqttPullJsonHelper.readArrayElements(body, routing.getResponseArrayJsonPath());
        if (elements.size() > 1) {
            return StringUtils.isNotBlank(routing.getDeviceIdJsonPath());
        }
        if (elements.size() == 1) {
            String externalId = MqttPullJsonHelper.readDeviceId(elements.get(0), routing.getDeviceIdJsonPath());
            return StringUtils.isNotBlank(externalId);
        }
        return false;
    }

    static boolean isPerMessageMode(HttpPullDeviceRoutingConfiguration routing) {
        return routing != null && routing.getRoutingMode() == HttpPullRoutingMode.PER_MESSAGE;
    }
}
