package com.jnks.iot.server.service.queue.processing;

import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.UUID;

public class SequentialByTenantIdJnksIotRuleEngineSubmitStrategy extends SequentialByEntityIdJnksIotRuleEngineSubmitStrategy {

    public SequentialByTenantIdJnksIotRuleEngineSubmitStrategy(String queueName) {
        super(queueName);
    }

    @Override
    protected EntityId getEntityId(TransportProtos.ToRuleEngineMsg msg) {
        return TenantId.fromUUID(new UUID(msg.getTenantIdMSB(), msg.getTenantIdLSB()));
    }
}
