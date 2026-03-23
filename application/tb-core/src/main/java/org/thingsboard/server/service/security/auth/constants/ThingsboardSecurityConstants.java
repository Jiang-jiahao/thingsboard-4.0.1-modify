package org.thingsboard.server.service.security.auth.constants;

public interface ThingsboardSecurityConstants {
    // JWT相关常量
    String JWT_TOKEN_HEADER_PARAM = "X-Authorization";
    String JWT_TOKEN_HEADER_PARAM_V2 = "Authorization";
    String JWT_TOKEN_QUERY_PARAM = "token";

    // 设备API入口
    String DEVICE_API_ENTRY_POINT = "/api/v1/**";

    // 普通登录入口
    String FORM_BASED_LOGIN_ENTRY_POINT = "/api/auth/login";

    // 公共登录入口
    String PUBLIC_LOGIN_ENTRY_POINT = "/api/auth/login/public";

    // Token刷新入口
    String TOKEN_REFRESH_ENTRY_POINT = "/api/auth/token";

    // 无需认证的静态资源
    String[] NON_TOKEN_BASED_AUTH_ENTRY_POINTS = new String[]{"/index.html", "/assets/**", "/static/**", "/api/noauth/**", "/webjars/**", "/api/license/**", "/api/images/public/**", "/.well-known/**"};

    // 需要认证的端点
    String TOKEN_BASED_AUTH_ENTRY_POINT = "/api/**";

    // WebSocket端点
    String WS_ENTRY_POINT = "/api/ws/**";

    // 邮件OAuth2处理端点
    String MAIL_OAUTH2_PROCESSING_ENTRY_POINT = "/api/admin/mail/oauth2/code";

    // 设备证书下载端点
    String DEVICE_CONNECTIVITY_CERTIFICATE_DOWNLOAD_ENTRY_POINT = "/api/device-connectivity/*/certificate/download";

}
