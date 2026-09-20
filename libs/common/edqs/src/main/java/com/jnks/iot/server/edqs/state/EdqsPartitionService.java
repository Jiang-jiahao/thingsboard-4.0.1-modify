package com.jnks.iot.server.edqs.state;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.queue.discovery.HashPartitionService;
import com.jnks.iot.server.queue.edqs.EdqsConfig;
import com.jnks.iot.server.queue.edqs.EdqsConfig.EdqsPartitioningStrategy;

@Service
@RequiredArgsConstructor
public class EdqsPartitionService {

    private final HashPartitionService hashPartitionService;
    private final EdqsConfig edqsConfig;

    public Integer resolvePartition(TenantId tenantId, Object key) {
        if (edqsConfig.getPartitioningStrategy() == EdqsPartitioningStrategy.TENANT) {
            return hashPartitionService.resolvePartitionIndex(tenantId.getId(), edqsConfig.getPartitions());
        } else {
            if (key == null) {
                throw new IllegalArgumentException("Partitioning key is missing but partitioning strategy is not TENANT");
            }
            return hashPartitionService.resolvePartitionIndex(key.toString(), edqsConfig.getPartitions());
        }
    }

}
