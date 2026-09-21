package com.jnks.iot.server.actors.calculatedField;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorException;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;
import com.jnks.iot.server.common.msg.cf.CalculatedFieldPartitionChangeMsg;

@Slf4j
public class CalculatedFieldEntityActor extends AbstractCalculatedFieldActor {

    private final CalculatedFieldEntityMessageProcessor processor;

    CalculatedFieldEntityActor(ActorSystemContext systemContext, TenantId tenantId, EntityId entityId) {
        super(systemContext, tenantId);
        this.processor = new CalculatedFieldEntityMessageProcessor(systemContext, tenantId, entityId);
    }

    @Override
    public void init(JnksIotActorCtx ctx) throws JnksIotActorException {
        super.init(ctx);
        log.debug("[{}][{}] Starting CF entity actor.", processor.tenantId, processor.entityId);
        try {
            processor.init(ctx);
            log.debug("[{}][{}] CF entity actor started.", processor.tenantId, processor.entityId);
        } catch (Exception e) {
            log.warn("[{}][{}] Unknown failure", processor.tenantId, processor.entityId, e);
            throw new JnksIotActorException("Failed to initialize CF entity actor", e);
        }
    }

    @Override
    public void destroy(JnksIotActorStopReason stopReason, Throwable cause) throws JnksIotActorException {
        log.debug("[{}] Stopping CF entity actor.", processor.tenantId);
        processor.stop();
    }

    @Override
    protected boolean doProcessCfMsg(ToCalculatedFieldSystemMsg msg) throws CalculatedFieldException {
        switch (msg.getMsgType()) {
            case CF_PARTITIONS_CHANGE_MSG:
                processor.process((CalculatedFieldPartitionChangeMsg) msg);
                break;
            case CF_STATE_RESTORE_MSG:
                processor.process((CalculatedFieldStateRestoreMsg) msg);
                break;
            case CF_ENTITY_INIT_CF_MSG:
                processor.process((EntityInitCalculatedFieldMsg) msg);
                break;
            case CF_ENTITY_DELETE_MSG:
                processor.process((CalculatedFieldEntityDeleteMsg) msg);
                break;
            case CF_ENTITY_TELEMETRY_MSG:
                processor.process((EntityCalculatedFieldTelemetryMsg) msg);
                break;
            case CF_LINKED_TELEMETRY_MSG:
                processor.process((EntityCalculatedFieldLinkedTelemetryMsg) msg);
                break;
            default:
                return false;
        }
        return true;
    }

    @Override
    void logProcessingException(Exception e) {
        log.warn("[{}][{}] Processing failure", tenantId, processor.entityId, e);
    }
}
