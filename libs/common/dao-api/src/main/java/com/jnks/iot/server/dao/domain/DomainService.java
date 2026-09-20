package com.jnks.iot.server.dao.domain;

import com.jnks.iot.server.common.data.domain.Domain;
import com.jnks.iot.server.common.data.domain.DomainInfo;
import com.jnks.iot.server.common.data.id.DomainId;
import com.jnks.iot.server.common.data.id.OAuth2ClientId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.List;

public interface DomainService extends EntityDaoService {

    Domain saveDomain(TenantId tenantId, Domain domain);

    void deleteDomainById(TenantId tenantId, DomainId domainId);

    Domain findDomainById(TenantId tenantId, DomainId domainId);

    PageData<DomainInfo> findDomainInfosByTenantId(TenantId tenantId, PageLink pageLink);

    DomainInfo findDomainInfoById(TenantId tenantId, DomainId domainId);

    boolean isOauth2Enabled(TenantId tenantId);

    void updateOauth2Clients(TenantId tenantId, DomainId domainId, List<OAuth2ClientId> oAuth2ClientIds);

    void deleteDomainsByTenantId(TenantId tenantId);
}
