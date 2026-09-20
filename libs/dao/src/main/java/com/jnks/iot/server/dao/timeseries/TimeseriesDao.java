package com.jnks.iot.server.dao.timeseries;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.DeleteTsKvQuery;
import com.jnks.iot.server.common.data.kv.ReadTsKvQuery;
import com.jnks.iot.server.common.data.kv.ReadTsKvQueryResult;
import com.jnks.iot.server.common.data.kv.TsKvEntry;

import java.util.List;

/**
 * @author Andrew Shvayka
 */
public interface TimeseriesDao {

    ListenableFuture<List<ReadTsKvQueryResult>> findAllAsync(TenantId tenantId, EntityId entityId, List<ReadTsKvQuery> queries);

    ListenableFuture<Integer> save(TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry, long ttl);

    ListenableFuture<Integer> savePartition(TenantId tenantId, EntityId entityId, long tsKvEntryTs, String key);

    ListenableFuture<Void> remove(TenantId tenantId, EntityId entityId, DeleteTsKvQuery query);

    void cleanup(long systemTtl);
}
