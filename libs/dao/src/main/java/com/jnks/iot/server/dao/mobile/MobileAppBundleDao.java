package com.jnks.iot.server.dao.mobile;

import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundle;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundleInfo;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundleOauth2Client;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

import java.util.List;

public interface MobileAppBundleDao extends Dao<MobileAppBundle> {

    PageData<MobileAppBundleInfo> findInfosByTenantId(TenantId tenantId, PageLink pageLink);

    MobileAppBundleInfo findInfoById(TenantId tenantId, MobileAppBundleId mobileAppBundleId);

    List<MobileAppBundleOauth2Client> findOauth2ClientsByMobileAppBundleId(TenantId tenantId, MobileAppBundleId mobileAppBundleId);

    void addOauth2Client(TenantId tenantId, MobileAppBundleOauth2Client mobileAppBundleOauth2Client);

    void removeOauth2Client(TenantId tenantId, MobileAppBundleOauth2Client mobileAppBundleOauth2Client);

    MobileAppBundle findByPkgNameAndPlatform(TenantId tenantId, String pkgName, PlatformType platform);

    void deleteByTenantId(TenantId tenantId);

}
