package com.jnks.iot.server.edqs.repo;

import com.jnks.iot.server.common.data.edqs.EdqsEvent;
import com.jnks.iot.server.common.data.edqs.query.QueryResult;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.query.EntityCountQuery;
import com.jnks.iot.server.common.data.query.EntityDataQuery;

import java.util.function.Predicate;

public interface EdqsRepository {

    void processEvent(EdqsEvent event);

    long countEntitiesByQuery(TenantId tenantId, CustomerId customerId, EntityCountQuery query, boolean ignorePermissionCheck);

    PageData<QueryResult> findEntityDataByQuery(TenantId tenantId, CustomerId customerId, EntityDataQuery query, boolean ignorePermissionCheck);

    void clearIf(Predicate<TenantId> predicate);

    void clear();

}
