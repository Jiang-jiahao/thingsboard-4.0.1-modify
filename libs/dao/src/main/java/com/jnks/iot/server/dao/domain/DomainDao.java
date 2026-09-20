package com.jnks.iot.server.dao.domain;

import com.jnks.iot.server.common.data.domain.Domain;
import com.jnks.iot.server.common.data.domain.DomainOauth2Client;
import com.jnks.iot.server.common.data.id.DomainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

import java.util.List;

public interface DomainDao extends Dao<Domain> {

    PageData<Domain> findByTenantId(TenantId tenantId, PageLink pageLink);

    int countDomainByTenantIdAndOauth2Enabled(TenantId tenantId, boolean oauth2Enabled);

    List<DomainOauth2Client> findOauth2ClientsByDomainId(TenantId tenantId, DomainId domainId);

    void addOauth2Client(DomainOauth2Client domainOauth2Client);

    void removeOauth2Client(DomainOauth2Client domainOauth2Client);

    void deleteByTenantId(TenantId tenantId);
}
