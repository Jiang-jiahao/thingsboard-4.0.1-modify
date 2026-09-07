/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;

import java.util.List;

final class HttpPullRoutingHelper {

    private HttpPullRoutingHelper() {
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
        if (mode == HttpPullRoutingMode.MULTI_DEVICE) {
            return StringUtils.isNotBlank(routing.getDeviceIdJsonPath());
        }
        List<Object> elements = HttpPullJsonHelper.readArrayElements(body, routing.getResponseArrayJsonPath());
        if (elements.size() > 1) {
            return StringUtils.isNotBlank(routing.getDeviceIdJsonPath());
        }
        if (elements.size() == 1) {
            String externalId = HttpPullJsonHelper.readDeviceId(elements.get(0), routing.getDeviceIdJsonPath());
            return StringUtils.isNotBlank(externalId);
        }
        return false;
    }
}
