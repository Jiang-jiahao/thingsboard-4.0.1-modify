package com.jnks.iot.server.dao.sql.query;

import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.query.AlarmCountQuery;
import com.jnks.iot.server.common.data.query.AlarmData;
import com.jnks.iot.server.common.data.query.AlarmDataQuery;

import java.util.Collection;

public interface AlarmQueryRepository {

    PageData<AlarmData> findAlarmDataByQueryForEntities(TenantId tenantId,
                                                        AlarmDataQuery query, Collection<EntityId> orderedEntityIds);

    long countAlarmsByQuery(TenantId tenantId, CustomerId customerId, AlarmCountQuery query, Collection<EntityId> orderedEntityIds);

}
