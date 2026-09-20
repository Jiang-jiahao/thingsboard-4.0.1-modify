package com.jnks.iot.server.common.data.queue;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.BaseData;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.id.QueueStatsId;
import com.jnks.iot.server.common.data.id.TenantId;

@EqualsAndHashCode(callSuper = true)
@Data
public class QueueStats extends BaseData<QueueStatsId> implements HasTenantId {
    private TenantId tenantId;
    private String queueName;
    private String serviceId;

    public QueueStats() {
    }

    public QueueStats(QueueStatsId id) {
        super(id);
    }

}
