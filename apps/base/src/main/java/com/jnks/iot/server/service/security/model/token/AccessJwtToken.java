package com.jnks.iot.server.service.security.model.token;

import com.jnks.iot.server.common.data.security.model.JwtToken;

public final class AccessJwtToken implements JwtToken {
    private final String rawToken;

    public AccessJwtToken(String rawToken) {
        this.rawToken = rawToken;
    }

    public String getToken() {
        return this.rawToken;
    }

}
