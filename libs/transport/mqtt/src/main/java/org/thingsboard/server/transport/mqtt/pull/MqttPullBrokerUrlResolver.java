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

public final class MqttPullBrokerUrlResolver {

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

    private static URI toUri(String value) throws URISyntaxException {
        if (value.contains("://")) {
            return new URI(value);
        }
        return new URI("tcp://" + value);
    }
}
