package com.jnks.iot.server.common.data.mobile.bundle;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.OAuth2ClientId;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MobileAppBundleOauth2Client {

    private MobileAppBundleId mobileAppBundleId;
    private OAuth2ClientId oAuth2ClientId;

}
