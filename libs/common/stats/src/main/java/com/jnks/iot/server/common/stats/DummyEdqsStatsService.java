package com.jnks.iot.server.common.stats;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.ObjectType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.query.EntityCountQuery;
import com.jnks.iot.server.common.data.query.EntityDataQuery;

@Service
@ConditionalOnMissingBean(value = EdqsStatsService.class, ignored = DummyEdqsStatsService.class)
public class DummyEdqsStatsService implements EdqsStatsService {

    @Override
    public void reportAdded(ObjectType objectType) {}

    @Override
    public void reportRemoved(ObjectType objectType) {}

    @Override
    public void reportEntityDataQuery(TenantId tenantId, EntityDataQuery query, long timingNanos) {}

    @Override
    public void reportEntityCountQuery(TenantId tenantId, EntityCountQuery query, long timingNanos) {}

    @Override
    public void reportEdqsDataQuery(TenantId tenantId, EntityDataQuery query, long timingNanos) {}

    @Override
    public void reportEdqsCountQuery(TenantId tenantId, EntityCountQuery query, long timingNanos) {}

    @Override
    public void reportStringCompressed() {}

    @Override
    public void reportStringUncompressed() {}

}
