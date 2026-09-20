package com.jnks.iot.server.common.stats;

import com.jnks.iot.server.common.data.ObjectType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.query.EntityCountQuery;
import com.jnks.iot.server.common.data.query.EntityDataQuery;

public interface EdqsStatsService {

    void reportAdded(ObjectType objectType);

    void reportRemoved(ObjectType objectType);

    void reportEntityDataQuery(TenantId tenantId, EntityDataQuery query, long timingNanos);

    void reportEntityCountQuery(TenantId tenantId, EntityCountQuery query, long timingNanos);

    void reportEdqsDataQuery(TenantId tenantId, EntityDataQuery query, long timingNanos);

    void reportEdqsCountQuery(TenantId tenantId, EntityCountQuery query, long timingNanos);

    void reportStringCompressed();

    void reportStringUncompressed();

}
