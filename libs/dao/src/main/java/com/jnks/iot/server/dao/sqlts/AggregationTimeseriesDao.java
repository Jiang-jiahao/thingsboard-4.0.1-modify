package com.jnks.iot.server.dao.sqlts;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.ReadTsKvQuery;
import com.jnks.iot.server.common.data.kv.ReadTsKvQueryResult;

public interface AggregationTimeseriesDao {

    ListenableFuture<ReadTsKvQueryResult> findAllAsync(TenantId tenantId, EntityId entityId, ReadTsKvQuery query);
}