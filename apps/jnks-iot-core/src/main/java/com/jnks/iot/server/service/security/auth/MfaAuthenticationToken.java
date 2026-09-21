package com.jnks.iot.server.service.security.auth;

import com.jnks.iot.server.service.security.model.SecurityUser;

public class MfaAuthenticationToken extends AbstractJwtAuthenticationToken {
    public MfaAuthenticationToken(SecurityUser securityUser) {
        super(securityUser);
    }
}
