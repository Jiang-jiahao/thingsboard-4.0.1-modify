/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.transport.http.HttpPullAuthConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullAuthType;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class HttpPullAuthService {

    private final HttpPullHttpClient sharedHttpClient = new HttpPullHttpClient(10000);
    private final Map<DeviceId, TokenState> tokenCache = new ConcurrentHashMap<>();

    public AuthRequestContext prepareAuth(DeviceId collectorId, HttpPullAuthConfiguration auth, String pollUrl,
                                          boolean requiresAuth) throws Exception {
        if (!requiresAuth || auth == null || auth.getAuthType() == null || auth.getAuthType() == HttpPullAuthType.NONE) {
            return AuthRequestContext.builder().url(pollUrl).headers(new HashMap<>()).build();
        }
        Map<String, String> headers = new HashMap<>();
        String url = pollUrl;
        Map<String, String> queryParams = new HashMap<>();
        switch (auth.getAuthType()) {
            case API_KEY -> applyApiKey(auth, headers, queryParams);
            case BASIC -> headers.put("Authorization", basicHeader(auth.getUsername(), auth.getPassword()));
            case BEARER_STATIC -> headers.put(auth.getTokenHeader() != null ? auth.getTokenHeader() : "Authorization",
                    prefix(auth.getTokenPrefix(), "Bearer ") + auth.getBearerToken());
            case LOGIN_TOKEN -> applyLoginToken(collectorId, auth, headers);
            case OAUTH2_CLIENT_CREDENTIALS, OAUTH2_PASSWORD -> applyOAuth2(collectorId, auth, headers);
            default -> {
            }
        }
        if (Boolean.TRUE.equals(auth.getApiKeyInQuery()) && auth.getAuthType() == HttpPullAuthType.API_KEY) {
            return AuthRequestContext.builder().url(url).headers(headers).queryParams(queryParams).build();
        }
        return AuthRequestContext.builder().url(url).headers(headers).queryParams(queryParams.isEmpty() ? null : queryParams).build();
    }

    private void applyApiKey(HttpPullAuthConfiguration auth, Map<String, String> headers, Map<String, String> queryParams) {
        if (Boolean.TRUE.equals(auth.getApiKeyInQuery())) {
            queryParams.put(auth.getApiKeyQueryParam() != null ? auth.getApiKeyQueryParam() : "apiKey", auth.getApiKeyValue());
        } else {
            headers.put(auth.getApiKeyHeader() != null ? auth.getApiKeyHeader() : "X-API-Key", auth.getApiKeyValue());
        }
    }

    private void applyLoginToken(DeviceId collectorId, HttpPullAuthConfiguration auth, Map<String, String> headers) throws Exception {
        String token = resolveToken(collectorId, auth, false);
        headers.put(auth.getTokenHeader() != null ? auth.getTokenHeader() : "Authorization",
                prefix(auth.getTokenPrefix(), "Bearer ") + token);
    }

    private void applyOAuth2(DeviceId collectorId, HttpPullAuthConfiguration auth, Map<String, String> headers) throws Exception {
        String token = resolveToken(collectorId, auth, true);
        headers.put("Authorization", "Bearer " + token);
    }

    private String resolveToken(DeviceId collectorId, HttpPullAuthConfiguration auth, boolean oauth) throws Exception {
        TokenState state = tokenCache.get(collectorId);
        long now = System.currentTimeMillis();
        if (state != null && state.expiresAtMs > now + 5000) {
            return state.accessToken;
        }
        synchronized (collectorId) {
            state = tokenCache.computeIfAbsent(collectorId, k -> new TokenState());
            if (state.expiresAtMs > now + 5000 && StringUtils.isNotBlank(state.accessToken)) {
                return state.accessToken;
            }
            if (!oauth && StringUtils.isNotBlank(state.refreshToken) && StringUtils.isNotBlank(auth.getRefreshUrl())) {
                refreshLoginToken(collectorId, auth, state);
            } else if (oauth && StringUtils.isNotBlank(state.refreshToken)) {
                refreshOAuthToken(collectorId, auth, state);
            } else {
                fetchNewToken(collectorId, auth, oauth, state);
            }
            return state.accessToken;
        }
    }

    private void fetchNewToken(DeviceId collectorId, HttpPullAuthConfiguration auth, boolean oauth, TokenState state) throws Exception {
        if (state == null) {
            state = new TokenState();
            tokenCache.put(collectorId, state);
        }
        if (oauth) {
            fetchOAuthToken(auth, state);
        } else {
            fetchLoginToken(auth, state);
        }
    }

    private void fetchLoginToken(HttpPullAuthConfiguration auth, TokenState state) throws Exception {
        Map<String, String> headers = new HashMap<>();
        if (auth.getLoginHeaders() != null) {
            headers.putAll(auth.getLoginHeaders());
        }
        if (!headers.containsKey("Content-Type")) {
            headers.put("Content-Type", "application/json");
        }
        HttpPullHttpClient.HttpPullResponse response = sharedHttpClient.execute(HttpPullHttpClient.HttpPullRequest.builder()
                .url(auth.getLoginUrl())
                .method(auth.getLoginMethod())
                .body(auth.getLoginBody())
                .headers(headers)
                .readTimeoutMs(10000)
                .build());
        applyTokenFromBody(auth, response.getBody(), state);
    }

    private void refreshLoginToken(DeviceId collectorId, HttpPullAuthConfiguration auth, TokenState state) throws Exception {
        String body = auth.getRefreshBodyTemplate();
        if (StringUtils.isBlank(body)) {
            JsonObject o = new JsonObject();
            o.addProperty("refreshToken", state.refreshToken);
            body = o.toString();
        } else {
            body = body.replace("${refreshToken}", state.refreshToken);
        }
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        HttpPullHttpClient.HttpPullResponse response = sharedHttpClient.execute(HttpPullHttpClient.HttpPullRequest.builder()
                .url(auth.getRefreshUrl())
                .method(auth.getRefreshMethod() != null ? auth.getRefreshMethod() : "POST")
                .body(body)
                .headers(headers)
                .readTimeoutMs(10000)
                .build());
        applyTokenFromBody(auth, response.getBody(), state);
    }

    private void fetchOAuthToken(HttpPullAuthConfiguration auth, TokenState state) throws Exception {
        String body = buildOAuthBody(auth, false);
        Map<String, String> headers = Map.of("Content-Type", "application/x-www-form-urlencoded");
        HttpPullHttpClient.HttpPullResponse response = sharedHttpClient.execute(HttpPullHttpClient.HttpPullRequest.builder()
                .url(auth.getTokenUrl())
                .method("POST")
                .body(body)
                .headers(headers)
                .readTimeoutMs(10000)
                .build());
        applyOAuthFromBody(auth, response.getBody(), state);
    }

    private void refreshOAuthToken(DeviceId collectorId, HttpPullAuthConfiguration auth, TokenState state) throws Exception {
        String body = "grant_type=refresh_token&refresh_token=" + urlEncode(state.refreshToken)
                + "&client_id=" + urlEncode(auth.getClientId())
                + "&client_secret=" + urlEncode(auth.getClientSecret());
        HttpPullHttpClient.HttpPullResponse response = sharedHttpClient.execute(HttpPullHttpClient.HttpPullRequest.builder()
                .url(auth.getTokenUrl())
                .method("POST")
                .body(body)
                .headers(Map.of("Content-Type", "application/x-www-form-urlencoded"))
                .readTimeoutMs(10000)
                .build());
        applyOAuthFromBody(auth, response.getBody(), state);
    }

    private static String buildOAuthBody(HttpPullAuthConfiguration auth, boolean refresh) {
        if (refresh) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (auth.getAuthType() == HttpPullAuthType.OAUTH2_PASSWORD) {
            sb.append("grant_type=password");
            sb.append("&username=").append(urlEncode(auth.getOauthUsername()));
            sb.append("&password=").append(urlEncode(auth.getOauthPassword()));
        } else {
            sb.append("grant_type=client_credentials");
        }
        sb.append("&client_id=").append(urlEncode(auth.getClientId()));
        sb.append("&client_secret=").append(urlEncode(auth.getClientSecret()));
        if (StringUtils.isNotBlank(auth.getScope())) {
            sb.append("&scope=").append(urlEncode(auth.getScope()));
        }
        return sb.toString();
    }

    private void applyTokenFromBody(HttpPullAuthConfiguration auth, String body, TokenState state) {
        String path = StringUtils.isNotBlank(auth.getAccessTokenJsonPath()) ? auth.getAccessTokenJsonPath() : "$.token";
        String token = HttpPullJsonHelper.readJsonPath(body, path);
        if (StringUtils.isBlank(token)) {
            throw new IllegalStateException("HTTP pull login did not return credential at JSONPath " + path);
        }
        state.accessToken = token;
        if (StringUtils.isNotBlank(auth.getRefreshTokenJsonPath())) {
            state.refreshToken = HttpPullJsonHelper.readJsonPath(body, auth.getRefreshTokenJsonPath());
        }
        state.expiresAtMs = System.currentTimeMillis() + resolveTtlMs(auth, body);
    }

    private void applyOAuthFromBody(HttpPullAuthConfiguration auth, String body, TokenState state) {
        String token = HttpPullJsonHelper.readJsonPath(body, "$.access_token");
        if (StringUtils.isBlank(token)) {
            token = HttpPullJsonHelper.readJsonPath(body, "$.token");
        }
        if (StringUtils.isBlank(token)) {
            throw new IllegalStateException("HTTP pull OAuth did not return access_token");
        }
        state.accessToken = token;
        state.refreshToken = HttpPullJsonHelper.readJsonPath(body, "$.refresh_token");
        String expiresIn = HttpPullJsonHelper.readJsonPath(body, "$.expires_in");
        long ttl = auth.getDefaultTokenTtlSec() != null ? auth.getDefaultTokenTtlSec() * 1000L : 3600_000L;
        if (StringUtils.isNotBlank(expiresIn)) {
            try {
                ttl = Long.parseLong(expiresIn) * 1000L;
            } catch (NumberFormatException ignored) {
            }
        }
        state.expiresAtMs = System.currentTimeMillis() + ttl;
    }

    private long resolveTtlMs(HttpPullAuthConfiguration auth, String body) {
        long defaultTtl = auth.getDefaultTokenTtlSec() != null ? auth.getDefaultTokenTtlSec() * 1000L : 3600_000L;
        if (StringUtils.isNotBlank(auth.getExpiresInJsonPath())) {
            String expires = HttpPullJsonHelper.readJsonPath(body, auth.getExpiresInJsonPath());
            if (StringUtils.isNotBlank(expires)) {
                try {
                    return Long.parseLong(expires) * 1000L;
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return defaultTtl;
    }

    public void invalidate(DeviceId collectorId) {
        tokenCache.remove(collectorId);
    }

    private static String basicHeader(String user, String pass) {
        String raw = user + ":" + pass;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private static String prefix(String configured, String fallback) {
        if (StringUtils.isBlank(configured)) {
            return fallback;
        }
        return configured.endsWith(" ") ? configured : configured + " ";
    }

    private static String urlEncode(String v) {
        return URLEncoder.encode(v != null ? v : "", StandardCharsets.UTF_8);
    }

    @Data
    @lombok.Builder
    public static class AuthRequestContext {
        private String url;
        private Map<String, String> headers;
        private Map<String, String> queryParams;
    }

    @Data
    private static class TokenState {
        private String accessToken;
        private String refreshToken;
        private long expiresAtMs;
    }
}
