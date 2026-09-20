package com.jnks.iot.server.dao.sql.mobile;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundle;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundleInfo;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundleOauth2Client;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.mobile.MobileAppBundleDao;
import com.jnks.iot.server.dao.model.sql.MobileAppBundleEntity;
import com.jnks.iot.server.dao.model.sql.MobileAppBundleOauth2ClientEntity;
import com.jnks.iot.server.dao.model.sql.MobileAppOauth2ClientCompositeKey;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@SqlDao
public class JpaMobileAppBundleDao extends JpaAbstractDao<MobileAppBundleEntity, MobileAppBundle> implements MobileAppBundleDao {

    private final MobileAppBundleRepository mobileAppBundleRepository;
    private final MobileAppBundleOauth2ClientRepository mobileOauth2ProviderRepository;

    @Override
    protected Class<MobileAppBundleEntity> getEntityClass() {
        return MobileAppBundleEntity.class;
    }

    @Override
    protected JpaRepository<MobileAppBundleEntity, UUID> getRepository() {
        return mobileAppBundleRepository;
    }

    @Override
    public PageData<MobileAppBundleInfo> findInfosByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(mobileAppBundleRepository.findInfoByTenantId(tenantId.getId(), pageLink.getTextSearch(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public MobileAppBundleInfo findInfoById(TenantId tenantId, MobileAppBundleId mobileAppBundleId) {
        return DaoUtil.getData(mobileAppBundleRepository.findInfoById(mobileAppBundleId.getId()));
    }

    @Override
    public List<MobileAppBundleOauth2Client> findOauth2ClientsByMobileAppBundleId(TenantId tenantId, MobileAppBundleId mobileAppBundleId) {
        return DaoUtil.convertDataList(mobileOauth2ProviderRepository.findAllByMobileAppBundleId(mobileAppBundleId.getId()));
    }

    @Override
    public void addOauth2Client(TenantId tenantId, MobileAppBundleOauth2Client mobileAppBundleOauth2Client) {
        mobileOauth2ProviderRepository.save(new MobileAppBundleOauth2ClientEntity(mobileAppBundleOauth2Client));
    }

    @Override
    public void removeOauth2Client(TenantId tenantId, MobileAppBundleOauth2Client mobileAppBundleOauth2Client) {
        mobileOauth2ProviderRepository.deleteById(new MobileAppOauth2ClientCompositeKey(mobileAppBundleOauth2Client.getMobileAppBundleId().getId(),
                mobileAppBundleOauth2Client.getOAuth2ClientId().getId()));
    }

    @Override
    public MobileAppBundle findByPkgNameAndPlatform(TenantId tenantId, String pkgName, PlatformType platform) {
        return DaoUtil.getData(mobileAppBundleRepository.findByPkgNameAndPlatformType(pkgName, platform));
    }

    @Override
    public void deleteByTenantId(TenantId tenantId) {
        mobileAppBundleRepository.deleteByTenantId(tenantId.getId());
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.MOBILE_APP_BUNDLE;
    }

}

