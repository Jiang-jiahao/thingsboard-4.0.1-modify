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
import org.thingsboard.server.common.data.transport.http.HttpPullPollRequest;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class HttpPullDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /**
     * UI 工作模式标记：PULL=主动拉取（与 {@link #getType()} 一致，便于 JSON 多态缺失时回显）。
     */
    private String httpTransportMode = "PULL";

    private Integer timeoutMs = 10000;
    private Integer readTimeoutMs = 10000;
    private Long queryingFrequencyMs = 30000L;

    private List<HttpPullPollRequest> pollRequests = new ArrayList<>();

    @Deprecated
    private String pollUrl;
    @Deprecated
    private String pollMethod = "GET";
    @Deprecated
    private String pollBody;
    @Deprecated
    private Map<String, String> pollHeaders = new HashMap<>();

    /** 档案级登录/令牌配置，供 requiresAuth=true 的拉取请求共用 */
    private HttpPullAuthConfiguration auth = new HttpPullAuthConfiguration();

    /** 兼容旧配置；新配置请写在各 {@link HttpPullPollRequest#getRouting()} */
    @Deprecated
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
    public List<HttpPullPollRequest> effectivePollRequests() {
        if (pollRequests != null && !pollRequests.isEmpty()) {
            return pollRequests.stream().filter(HttpPullPollRequest::isEnabled).toList();
        }
        if (StringUtils.isNotBlank(pollUrl)) {
            return List.of(HttpPullPollRequest.fromLegacyProfile(this));
        }
        return List.of();
    }

    @JsonIgnore
    public HttpPullDeviceRoutingConfiguration resolveRouting(HttpPullPollRequest request) {
        if (request.getRouting() != null) {
            return request.getRouting();
        }
        return routing;
    }

    @JsonIgnore
    public boolean needsMultiDeviceTargets() {
        for (HttpPullPollRequest request : effectivePollRequests()) {
            HttpPullDeviceRoutingConfiguration r = resolveRouting(request);
            if (r != null && (r.getRoutingMode() == HttpPullRoutingMode.MULTI_DEVICE
                    || r.getRoutingMode() == HttpPullRoutingMode.AUTO)) {
                return true;
            }
        }
        return false;
    }

    @JsonIgnore
    public long resolveQueryingFrequencyMs(HttpPullPollRequest request) {
        if (request.getQueryingFrequencyMs() != null && request.getQueryingFrequencyMs() > 0) {
            return request.getQueryingFrequencyMs();
        }
        return queryingFrequencyMs != null && queryingFrequencyMs > 0 ? queryingFrequencyMs : 30000L;
    }

    @JsonIgnore
    private boolean isValid() {
        if (timeoutMs == null || timeoutMs < 0 || readTimeoutMs == null || readTimeoutMs < 0) {
            return false;
        }
        if (queryingFrequencyMs == null || queryingFrequencyMs <= 0) {
            return false;
        }
        List<HttpPullPollRequest> requests = effectivePollRequests();
        if (requests.isEmpty()) {
            return false;
        }
        for (HttpPullPollRequest request : requests) {
            request.validate(routing);
        }
        if (auth != null) {
            auth.validate();
        }
        return true;
    }
}
