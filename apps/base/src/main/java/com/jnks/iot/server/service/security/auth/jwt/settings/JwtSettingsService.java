package com.jnks.iot.server.service.security.auth.jwt.settings;

import com.jnks.iot.server.common.data.security.model.JwtSettings;

public interface JwtSettingsService {

    String ADMIN_SETTINGS_JWT_KEY = "jwt";
    String TOKEN_SIGNING_KEY_DEFAULT = "jnksIotDefaultSigningKey";
    int TOKEN_SIGNING_KEY_MIN_SIZE_BITS = 512;

    JwtSettings getJwtSettings();

    JwtSettings reloadJwtSettings();

    JwtSettings saveJwtSettings(JwtSettings jwtSettings);

    @FunctionalInterface
    interface ReloadListener {
        void reload();
    }

}
