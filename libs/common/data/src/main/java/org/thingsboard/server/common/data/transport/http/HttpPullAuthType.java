package org.thingsboard.server.common.data.transport.http;

public enum HttpPullAuthType {
    NONE,
    API_KEY,
    BASIC,
    BEARER_STATIC,
    LOGIN_TOKEN,
    OAUTH2_CLIENT_CREDENTIALS,
    OAUTH2_PASSWORD
}
