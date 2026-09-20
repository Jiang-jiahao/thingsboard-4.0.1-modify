package com.jnks.iot.server.dao.service.validator;

import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.queue.QueueStats;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;

@Component
public class QueueStatsDataValidator extends DataValidator<QueueStats> {

    @Override
    protected void validateDataImpl(TenantId tenantId, QueueStats queueStats) {
        if (queueStats.getTenantId() == null) {
            throw new DataValidationException("Tenant id should be specified!.");
        }
        if (queueStats.getQueueName() == null) {
            throw new DataValidationException("Queue name should be specified!.");
        }
        if (StringUtils.isEmpty(queueStats.getServiceId())) {
            throw new DataValidationException("Service id should be specified!.");
        }
    }
}
