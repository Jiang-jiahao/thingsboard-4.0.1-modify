package com.jnks.iot.server.service.security.auth.oauth2;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import com.jnks.iot.server.common.data.oauth2.OAuth2Client;
import com.jnks.iot.server.service.security.model.SecurityUser;

public interface OAuth2ClientMapper {
    SecurityUser getOrCreateUserByClientPrincipal(HttpServletRequest request, OAuth2AuthenticationToken token, String providerAccessToken, OAuth2Client oAuth2Client);
}
