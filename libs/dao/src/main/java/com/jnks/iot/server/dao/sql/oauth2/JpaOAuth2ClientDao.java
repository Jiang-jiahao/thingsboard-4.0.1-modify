package com.jnks.iot.server.dao.sql.oauth2;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.OAuth2ClientId;
import com.jnks.iot.server.common.data.oauth2.OAuth2Client;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.model.sql.OAuth2ClientEntity;
import com.jnks.iot.server.dao.oauth2.OAuth2ClientDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.jnks.iot.server.dao.DaoUtil.toUUIDs;

@Component
@RequiredArgsConstructor
@SqlDao
public class JpaOAuth2ClientDao extends JpaAbstractDao<OAuth2ClientEntity, OAuth2Client> implements OAuth2ClientDao {

    private final OAuth2ClientRepository repository;

    @Override
    protected Class<OAuth2ClientEntity> getEntityClass() {
        return OAuth2ClientEntity.class;
    }

    @Override
    protected JpaRepository<OAuth2ClientEntity, UUID> getRepository() {
        return repository;
    }

    @Override
    public PageData<OAuth2Client> findByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(repository.findByTenantId(tenantId, pageLink.getTextSearch(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public List<OAuth2Client> findEnabledByDomainName(String domainName) {
        return DaoUtil.convertDataList(repository.findEnabledByDomainNameAndPlatformType(domainName, PlatformType.WEB.name()));
    }

    @Override
    public List<OAuth2Client> findEnabledByPkgNameAndPlatformType(String pkgName, PlatformType platformType) {
        List<OAuth2ClientEntity> clientEntities;
        if (platformType != null) {
            clientEntities = switch (platformType) {
                case ANDROID -> repository.findEnabledByAndroidPkgNameAndPlatformType(pkgName, platformType.name());
                case IOS -> repository.findEnabledByIosPkgNameAndPlatformType(pkgName, platformType.name());
                default -> Collections.emptyList();
            };
        } else {
            clientEntities = Collections.emptyList();
        }
        return DaoUtil.convertDataList(clientEntities);
    }

    @Override
    public List<OAuth2Client> findByDomainId(UUID oauth2ParamsId) {
        return DaoUtil.convertDataList(repository.findByDomainId(oauth2ParamsId));
    }

    @Override
    public List<OAuth2Client> findByMobileAppBundleId(UUID mobileAppBundleId) {
        return DaoUtil.convertDataList(repository.findByMobileAppBundleId(mobileAppBundleId));
    }

    @Override
    public String findAppSecret(UUID id, String pkgName, PlatformType platformType) {
        return repository.findAppSecret(id, pkgName, platformType);
    }

    @Override
    public void deleteByTenantId(UUID tenantId) {
        repository.deleteByTenantId(tenantId);
    }

    @Override
    public List<OAuth2Client> findByIds(UUID tenantId, List<OAuth2ClientId> oAuth2ClientIds) {
        return DaoUtil.convertDataList(repository.findByTenantIdAndIdIn(tenantId, toUUIDs(oAuth2ClientIds)));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.OAUTH2_CLIENT;
    }

}
