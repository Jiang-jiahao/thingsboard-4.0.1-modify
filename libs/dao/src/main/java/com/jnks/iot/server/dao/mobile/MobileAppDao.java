package com.jnks.iot.server.dao.mobile;

import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

public interface MobileAppDao extends Dao<MobileApp> {

    MobileApp findByBundleIdAndPlatformType(TenantId tenantId, MobileAppBundleId mobileAppBundleId, PlatformType platformType);

    PageData<MobileApp> findByTenantId(TenantId tenantId, PlatformType platformType, PageLink pageLink);

    void deleteByTenantId(TenantId tenantId);

    MobileApp findByPkgNameAndPlatformType(TenantId tenantId, String pkgName, PlatformType platform);
}
