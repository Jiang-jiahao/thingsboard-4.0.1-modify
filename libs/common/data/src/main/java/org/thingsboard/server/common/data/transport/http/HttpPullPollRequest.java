/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.http;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Data
public class HttpPullPollRequest implements Serializable {

    private String id;
    private String name;
    private Boolean enabled = true;

    private String pollUrl;
    private String pollMethod = "GET";
    private String pollBody;
    private Map<String, String> pollHeaders = new HashMap<>();

    /**
     * 轮询间隔（毫秒）；为空时使用档案级默认间隔。
     */
    private Long queryingFrequencyMs;

    private HttpPullPollDataType dataType = HttpPullPollDataType.TELEMETRY;

    /**
     * 是否携带档案鉴权（登录令牌等）。false 表示本请求无需令牌，例如登录接口本身。
     * 为空时：档案鉴权为 NONE 则为 false，否则为 true。
     */
    private Boolean requiresAuth;

    /**
     * 本请求响应体的解析与多设备路由（各请求 JSON 结构可不同）。
     */
    private HttpPullDeviceRoutingConfiguration routing = new HttpPullDeviceRoutingConfiguration();

    /** 运行时判断；不能叫 isEnabled()，否则 Jackson 会忽略 enabled 字段导致关闭状态无法保存。 */
    @JsonIgnore
    public boolean isRequestEnabled() {
        return enabled == null || enabled;
    }

    @JsonIgnore
    public boolean isRequiresAuth(HttpPullAuthConfiguration profileAuth) {
        if (requiresAuth != null) {
            return requiresAuth;
        }
        return profileAuth != null && profileAuth.getAuthType() != null
                && profileAuth.getAuthType() != HttpPullAuthType.NONE;
    }

    @JsonIgnore
    public void validate(HttpPullDeviceRoutingConfiguration profileRoutingFallback) {
        if (StringUtils.isBlank(pollUrl)) {
            throw new IllegalArgumentException("HTTP pull poll request requires pollUrl");
        }
        if (StringUtils.isBlank(pollMethod)) {
            throw new IllegalArgumentException("HTTP pull poll request requires pollMethod");
        }
        if (dataType == null) {
            dataType = HttpPullPollDataType.TELEMETRY;
        }
        if (queryingFrequencyMs != null && queryingFrequencyMs < 1000) {
            throw new IllegalArgumentException("HTTP pull poll queryingFrequencyMs must be >= 1000");
        }
        if (StringUtils.isBlank(id)) {
            id = UUID.randomUUID().toString();
        }
        HttpPullDeviceRoutingConfiguration effective = routing != null ? routing : profileRoutingFallback;
        if (effective != null) {
            effective.validate();
        }
    }

    public static HttpPullPollRequest fromLegacyProfile(HttpPullDeviceProfileTransportConfiguration profile) {
        HttpPullPollRequest request = new HttpPullPollRequest();
        request.setId(UUID.randomUUID().toString());
        request.setName("poll-1");
        request.setPollUrl(profile.getPollUrl());
        request.setPollMethod(profile.getPollMethod() != null ? profile.getPollMethod() : "GET");
        request.setPollBody(profile.getPollBody());
        if (profile.getPollHeaders() != null) {
            request.setPollHeaders(new HashMap<>(profile.getPollHeaders()));
        }
        request.setQueryingFrequencyMs(profile.getQueryingFrequencyMs());
        request.setDataType(HttpPullPollDataType.TELEMETRY);
        request.setRequiresAuth(null);
        if (profile.getRouting() != null) {
            request.setRouting(profile.getRouting());
        }
        return request;
    }
}
