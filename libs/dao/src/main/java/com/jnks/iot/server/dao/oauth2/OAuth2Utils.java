package com.jnks.iot.server.dao.oauth2;

import com.jnks.iot.server.common.data.oauth2.OAuth2ClientLoginInfo;
import com.jnks.iot.server.common.data.oauth2.OAuth2Client;

public class OAuth2Utils {
    public static final String OAUTH2_AUTHORIZATION_PATH_TEMPLATE = "/oauth2/authorization/%s";

    public static OAuth2ClientLoginInfo toClientLoginInfo(OAuth2Client registration) {
        OAuth2ClientLoginInfo client = new OAuth2ClientLoginInfo();
        client.setName(registration.getLoginButtonLabel());
        client.setUrl(String.format(OAUTH2_AUTHORIZATION_PATH_TEMPLATE, registration.getUuidId().toString()));
        client.setIcon(registration.getLoginButtonIcon());
        return client;
    }

}
