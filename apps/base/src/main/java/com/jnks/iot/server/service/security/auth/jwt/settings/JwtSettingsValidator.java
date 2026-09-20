package com.jnks.iot.server.service.security.auth.jwt.settings;

import com.jnks.iot.server.common.data.security.model.JwtSettings;

public interface JwtSettingsValidator {

    void validate(JwtSettings jwtSettings);
}
