package com.jnks.iot.server.common.data.mobile;

import com.jnks.iot.server.common.data.mobile.app.MobileAppVersionInfo;
import com.jnks.iot.server.common.data.mobile.app.StoreInfo;
import com.jnks.iot.server.common.data.oauth2.OAuth2ClientLoginInfo;

import java.util.List;

public record LoginMobileInfo(List<OAuth2ClientLoginInfo> oAuth2ClientLoginInfos,
                              StoreInfo storeInfo,
                              MobileAppVersionInfo versionInfo) {
}
