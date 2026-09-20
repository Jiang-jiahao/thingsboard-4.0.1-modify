package com.jnks.iot.server.dao.mobile;

import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.MobileAppId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;

public interface MobileAppService extends EntityDaoService {

    MobileApp saveMobileApp(TenantId tenantId, MobileApp mobileApp);

    MobileApp findMobileAppById(TenantId tenantId, MobileAppId mobileAppId);

    PageData<MobileApp> findMobileAppsByTenantId(TenantId tenantId, PlatformType platformType, PageLink pageLink);

    MobileApp findByBundleIdAndPlatformType(TenantId tenantId, MobileAppBundleId mobileAppBundleId, PlatformType platformType);

    MobileApp findMobileAppByPkgNameAndPlatformType(String pkgName, PlatformType platform);

    void deleteMobileAppById(TenantId tenantId, MobileAppId mobileAppId);

}
