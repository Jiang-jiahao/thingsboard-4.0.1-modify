package com.jnks.iot.server.dao.cf;

import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

import java.util.List;

public interface CalculatedFieldLinkDao extends Dao<CalculatedFieldLink> {

    List<CalculatedFieldLink> findCalculatedFieldLinksByCalculatedFieldId(TenantId tenantId, CalculatedFieldId calculatedFieldId);

    List<CalculatedFieldLink> findCalculatedFieldLinksByEntityId(TenantId tenantId, EntityId entityId);

    List<CalculatedFieldLink> findCalculatedFieldLinksByTenantId(TenantId tenantId);

    List<CalculatedFieldLink> findAll();

    PageData<CalculatedFieldLink> findAll(PageLink pageLink);

    PageData<CalculatedFieldLink> findAllByTenantId(TenantId tenantId, PageLink pageLink);

}
