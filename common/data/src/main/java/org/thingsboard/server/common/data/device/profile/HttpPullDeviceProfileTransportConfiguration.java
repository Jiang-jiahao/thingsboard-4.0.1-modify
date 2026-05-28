/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullAuthConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;

import java.util.HashMap;
import java.util.Map;

@Data
public class HttpPullDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    private Integer timeoutMs = 10000;
    private Integer readTimeoutMs = 10000;
    private Long queryingFrequencyMs = 30000L;

    /** 完整 URL 或相对 baseUrl 的路径 */
    private String pollUrl;
    private String pollMethod = "GET";
    private String pollBody;
    private Map<String, String> pollHeaders = new HashMap<>();

    private HttpPullAuthConfiguration auth = new HttpPullAuthConfiguration();
    private HttpPullDeviceRoutingConfiguration routing = new HttpPullDeviceRoutingConfiguration();

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.HTTP_PULL;
    }

    @Override
    public void validate() {
        if (!isValid()) {
            throw new IllegalArgumentException("HTTP pull transport configuration is not valid");
        }
    }

    @JsonIgnore
    private boolean isValid() {
        if (timeoutMs == null || timeoutMs < 0 || readTimeoutMs == null || readTimeoutMs < 0) {
            return false;
        }
        if (queryingFrequencyMs == null || queryingFrequencyMs <= 0) {
            return false;
        }
        if (StringUtils.isBlank(pollUrl)) {
            return false;
        }
        if (auth != null) {
            auth.validate();
        }
        if (routing != null) {
            routing.validate();
        } else {
            return false;
        }
        return true;
    }
}
