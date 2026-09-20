package com.jnks.iot.server.dao.mobile;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.MobileAppId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.AbstractEntityService;
import com.jnks.iot.server.dao.eventsourcing.DeleteEntityEvent;
import com.jnks.iot.server.dao.eventsourcing.SaveEntityEvent;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.service.Validator;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class MobileAppServiceImpl extends AbstractEntityService implements MobileAppService {

    private static final String PLATFORM_TYPE_IS_REQUIRED = "Platform type is required if package name is specified";

    @Autowired
    private MobileAppDao mobileAppDao;
    @Autowired
    private DataValidator<MobileApp> mobileAppDataValidator;

    @Override
    public MobileApp saveMobileApp(TenantId tenantId, MobileApp mobileApp) {
        log.trace("Executing saveMobileApp [{}]", mobileApp);
        mobileAppDataValidator.validate(mobileApp, a -> tenantId);
        try {
            MobileApp savedMobileApp = mobileAppDao.save(tenantId, mobileApp);
            eventPublisher.publishEvent(SaveEntityEvent.builder().tenantId(tenantId).entity(savedMobileApp).build());
            return savedMobileApp;
        } catch (Exception e) {
            checkConstraintViolation(e,
                    Map.of("mobile_app_pkg_name_platform_unq_key", "Mobile app with such package name and platform already exists!"));
            throw e;
        }
    }

    @Override
    public void deleteMobileAppById(TenantId tenantId, MobileAppId mobileAppId) {
        log.trace("Executing deleteMobileAppById [{}]", mobileAppId.getId());
        mobileAppDao.removeById(tenantId, mobileAppId.getId());
        eventPublisher.publishEvent(DeleteEntityEvent.builder().tenantId(tenantId).entityId(mobileAppId).build());
    }

    @Override
    public MobileApp findMobileAppById(TenantId tenantId, MobileAppId mobileAppId) {
        log.trace("Executing findMobileAppById [{}] [{}]", tenantId, mobileAppId);
        return mobileAppDao.findById(tenantId, mobileAppId.getId());
    }

    @Override
    public PageData<MobileApp> findMobileAppsByTenantId(TenantId tenantId, PlatformType platformType, PageLink pageLink) {
        log.trace("Executing findMobileAppInfosByTenantId [{}]", tenantId);
        return mobileAppDao.findByTenantId(tenantId, platformType, pageLink);
    }

    @Override
    public Optional<HasId<?>> findEntity(TenantId tenantId, EntityId entityId) {
        return Optional.ofNullable(findMobileAppById(tenantId, new MobileAppId(entityId.getId())));
    }

    @Override
    @Transactional
    public void deleteEntity(TenantId tenantId, EntityId id, boolean force) {
        deleteMobileAppById(tenantId, (MobileAppId) id);
    }

    @Override
    public MobileApp findByBundleIdAndPlatformType(TenantId tenantId, MobileAppBundleId mobileAppBundleId, PlatformType platformType) {
        log.trace("Executing findAndroidQrConfig, tenantId [{}], mobileAppBundleId [{}]", tenantId, mobileAppBundleId);
        return mobileAppDao.findByBundleIdAndPlatformType(tenantId, mobileAppBundleId, platformType);
    }

    @Override
    public MobileApp findMobileAppByPkgNameAndPlatformType(String pkgName, PlatformType platformType) {
        log.trace("Executing findMobileAppByPkgNameAndPlatformType, pkgName [{}], platform [{}]", pkgName, platformType);
        Validator.checkNotNull(platformType, PLATFORM_TYPE_IS_REQUIRED);
        return mobileAppDao.findByPkgNameAndPlatformType(TenantId.SYS_TENANT_ID, pkgName, platformType);
    }

    @Override
    public void deleteByTenantId(TenantId tenantId) {
        log.trace("Executing deleteByTenantId, tenantId [{}]", tenantId);
        mobileAppDao.deleteByTenantId(tenantId);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.MOBILE_APP;
    }
}
