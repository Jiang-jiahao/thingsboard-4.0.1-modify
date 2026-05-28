/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import org.thingsboard.server.common.data.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * 设备级 {@code pollUrlOverride}：可为完整 URL，或仅主机:端口（沿用档案 URL 的路径与查询串）。
 */
public final class HttpPullPollUrlResolver {

    private static final Pattern HOST_PORT = Pattern.compile("^[\\w.\\-]+:\\d+$");

    private HttpPullPollUrlResolver() {
    }

    public static String resolve(String profilePollUrl, String pollUrlOverride) {
        if (StringUtils.isBlank(pollUrlOverride)) {
            return profilePollUrl;
        }
        String trimmed = pollUrlOverride.trim();
        if (shouldMergeWithProfilePath(trimmed)) {
            return mergeHostWithProfilePath(trimmed, profilePollUrl);
        }
        return trimmed;
    }

    private static boolean shouldMergeWithProfilePath(String override) {
        if (HOST_PORT.matcher(override).matches()) {
            return true;
        }
        try {
            URI uri = toUri(override);
            String path = uri.getPath();
            return path == null || path.isEmpty() || "/".equals(path);
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private static String mergeHostWithProfilePath(String override, String profilePollUrl) {
        if (StringUtils.isBlank(profilePollUrl)) {
            return normalizeOrigin(override);
        }
        try {
            URI profile = new URI(profilePollUrl);
            URI origin = toUri(override);
            String path = profile.getRawPath() != null ? profile.getRawPath() : "";
            String query = profile.getRawQuery() != null ? "?" + profile.getRawQuery() : "";
            return origin.getScheme() + "://" + origin.getHost()
                    + (origin.getPort() > 0 ? ":" + origin.getPort() : "")
                    + path + query;
        } catch (URISyntaxException e) {
            return normalizeOrigin(override);
        }
    }

    private static URI toUri(String value) throws URISyntaxException {
        if (value.contains("://")) {
            return new URI(value);
        }
        return new URI("http://" + value);
    }

    private static String normalizeOrigin(String value) {
        try {
            URI uri = toUri(value);
            int port = uri.getPort();
            return uri.getScheme() + "://" + uri.getHost() + (port > 0 ? ":" + port : "");
        } catch (URISyntaxException e) {
            return value.startsWith("http") ? value : "http://" + value;
        }
    }
}
