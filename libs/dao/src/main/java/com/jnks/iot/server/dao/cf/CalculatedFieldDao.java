package com.jnks.iot.server.dao.cf;

import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.Dao;

import java.util.List;

public interface CalculatedFieldDao extends Dao<CalculatedField> {

    List<CalculatedField> findAllByTenantId(TenantId tenantId);

    List<CalculatedFieldId> findCalculatedFieldIdsByEntityId(TenantId tenantId, EntityId entityId);

    List<CalculatedField> findCalculatedFieldsByEntityId(TenantId tenantId, EntityId entityId);

    List<CalculatedField> findAll();

    PageData<CalculatedField> findAll(PageLink pageLink);

    PageData<CalculatedField> findAllByTenantId(TenantId tenantId, PageLink pageLink);

    PageData<CalculatedField> findAllByEntityId(TenantId tenantId, EntityId entityId, PageLink pageLink);

    List<CalculatedField> removeAllByEntityId(TenantId tenantId, EntityId entityId);

    long countCFByEntityId(TenantId tenantId, EntityId entityId);

}
