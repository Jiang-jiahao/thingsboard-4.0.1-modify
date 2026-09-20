package com.jnks.iot.server.dao.sql.mobile;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.mobile.MobileAppDao;
import com.jnks.iot.server.dao.model.sql.MobileAppEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.UUID;

@Component
@RequiredArgsConstructor
@SqlDao
public class JpaMobileAppDao extends JpaAbstractDao<MobileAppEntity, MobileApp> implements MobileAppDao {

    private final MobileAppRepository mobileAppRepository;

    @Override
    protected Class<MobileAppEntity> getEntityClass() {
        return MobileAppEntity.class;
    }

    @Override
    protected JpaRepository<MobileAppEntity, UUID> getRepository() {
        return mobileAppRepository;
    }

    @Override
    public MobileApp findByBundleIdAndPlatformType(TenantId tenantId, MobileAppBundleId mobileAppBundleId, PlatformType platformType) {
        return switch (platformType) {
            case ANDROID -> DaoUtil.getData(mobileAppRepository.findAndroidAppByBundleId(mobileAppBundleId.getId()));
            case IOS -> DaoUtil.getData(mobileAppRepository.findIOSAppByBundleId(mobileAppBundleId.getId()));
            default -> null;
        };
    }

    @Override
    public PageData<MobileApp> findByTenantId(TenantId tenantId, PlatformType platformType, PageLink pageLink) {
        return DaoUtil.toPageData(mobileAppRepository.findByTenantId(tenantId.getId(), platformType, pageLink.getTextSearch(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public void deleteByTenantId(TenantId tenantId) {
        mobileAppRepository.deleteByTenantId(tenantId.getId());
    }

    @Override
    public MobileApp findByPkgNameAndPlatformType(TenantId tenantId, String pkgName, PlatformType platform) {
        return DaoUtil.getData(mobileAppRepository.findByPkgNameAndPlatformType(pkgName, platform));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.MOBILE_APP;
    }

}

