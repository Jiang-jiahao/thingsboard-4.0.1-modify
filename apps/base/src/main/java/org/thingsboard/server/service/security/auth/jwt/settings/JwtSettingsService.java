package org.thingsboard.server.service.security.auth.jwt.settings;

import org.thingsboard.server.common.data.security.model.JwtSettings;

public interface JwtSettingsService {

    String ADMIN_SETTINGS_JWT_KEY = "jwt";
    String TOKEN_SIGNING_KEY_DEFAULT = "thingsboardDefaultSigningKey";
    int TOKEN_SIGNING_KEY_MIN_SIZE_BITS = 512;

    JwtSettings getJwtSettings();

    JwtSettings reloadJwtSettings();

    JwtSettings saveJwtSettings(JwtSettings jwtSettings);

    @FunctionalInterface
    interface ReloadListener {
        void reload();
    }

}
