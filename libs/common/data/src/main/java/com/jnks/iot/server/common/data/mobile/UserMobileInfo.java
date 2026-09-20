package com.jnks.iot.server.common.data.mobile;

import com.fasterxml.jackson.databind.JsonNode;
import com.jnks.iot.server.common.data.HomeDashboardInfo;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.mobile.app.MobileAppVersionInfo;
import com.jnks.iot.server.common.data.mobile.app.StoreInfo;


public record UserMobileInfo(User user,
                             StoreInfo storeInfo,
                             MobileAppVersionInfo versionInfo,
                             HomeDashboardInfo homeDashboardInfo,
                             JsonNode pages) {
}
