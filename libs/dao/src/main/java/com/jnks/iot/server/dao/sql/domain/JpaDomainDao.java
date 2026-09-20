package com.jnks.iot.server.dao.sql.domain;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.domain.Domain;
import com.jnks.iot.server.common.data.domain.DomainOauth2Client;
import com.jnks.iot.server.common.data.id.DomainId;
import com.jnks.iot.server.common.data.id.OAuth2ClientId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.domain.DomainDao;
import com.jnks.iot.server.dao.model.sql.DomainEntity;
import com.jnks.iot.server.dao.model.sql.DomainOauth2ClientCompositeKey;
import com.jnks.iot.server.dao.model.sql.DomainOauth2ClientEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@SqlDao
public class JpaDomainDao extends JpaAbstractDao<DomainEntity, Domain> implements DomainDao {

    private final DomainRepository domainRepository;
    private final DomainOauth2ClientRepository domainOauth2ClientRepository;

    @Override
    protected Class<DomainEntity> getEntityClass() {
        return DomainEntity.class;
    }

    @Override
    protected JpaRepository<DomainEntity, UUID> getRepository() {
        return domainRepository;
    }

    @Override
    public PageData<Domain> findByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(domainRepository.findByTenantId(tenantId.getId(), pageLink.getTextSearch(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public int countDomainByTenantIdAndOauth2Enabled(TenantId tenantId, boolean enabled) {
        return domainRepository.countByTenantIdAndOauth2Enabled(tenantId.getId(), enabled);
    }

    @Override
    public List<DomainOauth2Client> findOauth2ClientsByDomainId(TenantId tenantId, DomainId domainId) {
        return DaoUtil.convertDataList(domainOauth2ClientRepository.findAllByDomainId(domainId.getId()));
    }

    @Override
    public void addOauth2Client(DomainOauth2Client domainOauth2Client) {
        domainOauth2ClientRepository.save(new DomainOauth2ClientEntity(domainOauth2Client));
    }

    @Override
    public void removeOauth2Client(DomainOauth2Client domainOauth2Client) {
        domainOauth2ClientRepository.deleteById(new DomainOauth2ClientCompositeKey(domainOauth2Client.getDomainId().getId(),
                domainOauth2Client.getOAuth2ClientId().getId()));
    }

    @Override
    public void deleteByTenantId(TenantId tenantId) {
        domainRepository.deleteByTenantId(tenantId.getId());
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.DOMAIN;
    }
}

