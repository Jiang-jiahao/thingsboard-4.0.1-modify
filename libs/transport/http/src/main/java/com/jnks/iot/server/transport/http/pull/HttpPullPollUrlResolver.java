package com.jnks.iot.server.transport.http.pull;

import com.jnks.iot.server.common.data.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * 设备级 {@code pollUrlOverride}：可为完整 URL，或仅主机:端口（沿用档案 URL 的路径与查询串）。
 * <p>
 * 档案侧通常只配路径（真实 host:port 由设备提供），因此只写 {@code api/data} 这种缺前导斜杠的路径也要能拼对。
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

    /**
     * 设备 IP 覆盖：只替换 URL 的 host，保留协议、端口、路径与查询串。
     * <p>
     * 用于同一档案服务多台设备（端口/路径固定、IP 不同）。传入 {@code IP:端口} 或完整 URL 时
     * 按 {@link #resolve(String, String)} 处理，兼容既有的覆盖地址写法。
     */
    public static String resolveWithIp(String url, String ipOverride) {
        if (StringUtils.isBlank(url) || StringUtils.isBlank(ipOverride)) {
            return url;
        }
        String trimmed = ipOverride.trim();
        if (trimmed.contains("://") || trimmed.contains(":")) {
            return resolve(url, trimmed);
        }
        try {
            URI uri = new URI(url);
            return new URI(uri.getScheme(), uri.getUserInfo(), trimmed, uri.getPort(),
                    uri.getPath(), uri.getQuery(), uri.getFragment()).toString();
        } catch (URISyntaxException e) {
            return url;
        }
    }

    /**
     * 解析后的 URL 能否真正发起请求：必须有 scheme 与 host。
     * 档案只配路径又没配设备 host:port 时，拼出来的是相对路径，逐次轮询都会失败。
     */
    public static boolean isAbsolute(String url) {
        if (StringUtils.isBlank(url) || !url.contains("://")) {
            return false;
        }
        try {
            URI uri = new URI(url);
            return StringUtils.isNotBlank(uri.getScheme()) && StringUtils.isNotBlank(uri.getHost());
        } catch (URISyntaxException e) {
            return false;
        }
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
            String path = normalizePath(profile.getRawPath());
            String query = profile.getRawQuery() != null ? "?" + profile.getRawQuery() : "";
            return origin.getScheme() + "://" + origin.getHost()
                    + (origin.getPort() > 0 ? ":" + origin.getPort() : "")
                    + path + query;
        } catch (URISyntaxException e) {
            return normalizeOrigin(override);
        }
    }

    private static String normalizePath(String path) {
        if (StringUtils.isBlank(path)) {
            return "";
        }
        return path.startsWith("/") ? path : "/" + path;
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
