/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
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
