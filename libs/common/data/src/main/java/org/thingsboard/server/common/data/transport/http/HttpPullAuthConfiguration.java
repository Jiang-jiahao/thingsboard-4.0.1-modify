/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.server.common.data.StringUtils;

import java.io.Serializable;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class HttpPullAuthConfiguration implements Serializable {

    private HttpPullAuthType authType = HttpPullAuthType.NONE;

    // API_KEY
    private String apiKeyHeader = "X-API-Key";
    private String apiKeyValue;
    private Boolean apiKeyInQuery;
    private String apiKeyQueryParam = "apiKey";

    // BASIC
    private String username;
    private String password;

    // BEARER_STATIC
    private String bearerToken;

    // LOGIN_TOKEN
    private String loginUrl;
    private String loginMethod = "POST";
    private String loginBody;
    private Map<String, String> loginHeaders;
    /** JSONPath，如 $.token 或 $.data.accessToken */
    private String accessTokenJsonPath = "$.token";
    private String tokenHeader = "Authorization";
    private String tokenPrefix = "Bearer ";
    /** 可选：$.expiresIn（秒） */
    private String expiresInJsonPath;
    private Long defaultTokenTtlSec = 3600L;
    /** 可选 refresh：$.refreshToken */
    private String refreshTokenJsonPath;
    private String refreshUrl;
    private String refreshMethod = "POST";
    private String refreshBodyTemplate;

    // OAUTH2_CLIENT_CREDENTIALS / OAUTH2_PASSWORD
    private String tokenUrl;
    private String clientId;
    private String clientSecret;
    private String scope;
    /** OAUTH2_PASSWORD */
    private String oauthUsername;
    private String oauthPassword;

    public void validate() {
        if (authType == null) {
            authType = HttpPullAuthType.NONE;
        }
        switch (authType) {
            case NONE -> {
            }
            case API_KEY -> {
                if (StringUtils.isBlank(apiKeyValue)) {
                    throw new IllegalArgumentException("HTTP pull API key value is required");
                }
            }
            case BASIC -> {
                if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
                    throw new IllegalArgumentException("HTTP pull basic auth username/password are required");
                }
            }
            case BEARER_STATIC -> {
                if (StringUtils.isBlank(bearerToken)) {
                    throw new IllegalArgumentException("HTTP pull bearer token is required");
                }
            }
            case LOGIN_TOKEN -> {
                if (StringUtils.isBlank(loginUrl) || StringUtils.isBlank(accessTokenJsonPath)) {
                    throw new IllegalArgumentException("HTTP pull login URL and accessTokenJsonPath are required");
                }
            }
            case OAUTH2_CLIENT_CREDENTIALS, OAUTH2_PASSWORD -> {
                if (StringUtils.isBlank(tokenUrl) || StringUtils.isBlank(clientId) || StringUtils.isBlank(clientSecret)) {
                    throw new IllegalArgumentException("HTTP pull OAuth2 tokenUrl, clientId and clientSecret are required");
                }
                if (authType == HttpPullAuthType.OAUTH2_PASSWORD
                        && (StringUtils.isBlank(oauthUsername) || StringUtils.isBlank(oauthPassword))) {
                    throw new IllegalArgumentException("HTTP pull OAuth2 password grant requires oauthUsername and oauthPassword");
                }
            }
        }
    }
}
