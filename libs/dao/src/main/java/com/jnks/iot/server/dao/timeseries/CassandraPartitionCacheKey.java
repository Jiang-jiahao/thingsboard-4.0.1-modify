package com.jnks.iot.server.dao.timeseries;

import lombok.AllArgsConstructor;
import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;

@Data
@AllArgsConstructor
public class CassandraPartitionCacheKey {

    private EntityId entityId;
    private String key;
    private long partition;

}