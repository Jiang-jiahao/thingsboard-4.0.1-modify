package com.jnks.iot.server.dao.entity;

import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.query.EntityCountQuery;
import com.jnks.iot.server.common.data.query.EntityData;
import com.jnks.iot.server.common.data.query.EntityDataQuery;

public interface EntityQueryDao {

    long countEntitiesByQuery(TenantId tenantId, CustomerId customerId, EntityCountQuery query);

    PageData<EntityData> findEntityDataByQuery(TenantId tenantId, CustomerId customerId, EntityDataQuery query);

}
