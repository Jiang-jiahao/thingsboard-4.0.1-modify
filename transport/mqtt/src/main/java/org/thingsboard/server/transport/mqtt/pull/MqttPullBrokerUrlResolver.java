/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import lombok.Getter;
import org.thingsboard.server.common.data.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

public final class MqttPullBrokerUrlResolver {

    private static final Pattern HOST_PORT = Pattern.compile("^[\\w.\\-]+:\\d+$");

    private MqttPullBrokerUrlResolver() {
    }

    @Getter
    public static final class BrokerEndpoint {
        private final String host;
        private final int port;
        private final boolean ssl;

        public BrokerEndpoint(String host, int port, boolean ssl) {
            this.host = host;
            this.port = port;
            this.ssl = ssl;
        }
    }

    public static String resolve(String profileBrokerUrl, String brokerUrlOverride) {
        if (StringUtils.isBlank(brokerUrlOverride)) {
            return profileBrokerUrl;
        }
        String trimmed = brokerUrlOverride.trim();
        if (HOST_PORT.matcher(trimmed).matches()) {
            return mergeHostWithProfile(trimmed, profileBrokerUrl);
        }
        return trimmed;
    }

    public static BrokerEndpoint parse(String brokerUrl) {
        if (StringUtils.isBlank(brokerUrl)) {
            throw new IllegalArgumentException("MQTT broker URL is required");
        }
        try {
            URI uri = toUri(brokerUrl.trim());
            String scheme = uri.getScheme() != null ? uri.getScheme().toLowerCase() : "tcp";
            boolean ssl = "ssl".equals(scheme) || "mqtts".equals(scheme) || "tls".equals(scheme);
            String host = uri.getHost();
            if (StringUtils.isBlank(host)) {
                throw new IllegalArgumentException("Invalid MQTT broker URL: " + brokerUrl);
            }
            int port = uri.getPort();
            if (port <= 0) {
                port = ssl ? 8883 : 1883;
            }
            return new BrokerEndpoint(host, port, ssl);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid MQTT broker URL: " + brokerUrl, e);
        }
    }

    private static String mergeHostWithProfile(String override, String profileBrokerUrl) {
        if (StringUtils.isBlank(profileBrokerUrl)) {
            return normalizeBrokerUrl(override);
        }
        try {
            URI profile = toUri(profileBrokerUrl);
            URI origin = toUri(override);
            String scheme = profile.getScheme() != null ? profile.getScheme() : "tcp";
            int port = origin.getPort() > 0 ? origin.getPort() : profile.getPort();
            return scheme + "://" + origin.getHost() + (port > 0 ? ":" + port : "");
        } catch (URISyntaxException e) {
            return normalizeBrokerUrl(override);
        }
    }

    private static URI toUri(String value) throws URISyntaxException {
        if (value.contains("://")) {
            return new URI(value);
        }
        return new URI("tcp://" + value);
    }

    private static String normalizeBrokerUrl(String value) {
        try {
            URI uri = toUri(value);
            String scheme = uri.getScheme() != null ? uri.getScheme() : "tcp";
            int port = uri.getPort();
            return scheme + "://" + uri.getHost() + (port > 0 ? ":" + port : "");
        } catch (URISyntaxException e) {
            return value.startsWith("tcp") || value.startsWith("ssl") ? value : "tcp://" + value;
        }
    }
}
