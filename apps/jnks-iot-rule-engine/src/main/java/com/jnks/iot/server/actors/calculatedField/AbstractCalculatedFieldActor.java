package com.jnks.iot.server.actors.calculatedField;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.DebugModeUtil;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.service.ContextAwareActor;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.ToCalculatedFieldSystemMsg;

@Slf4j
public abstract class AbstractCalculatedFieldActor extends ContextAwareActor {

    protected final TenantId tenantId;

    public AbstractCalculatedFieldActor(ActorSystemContext systemContext, TenantId tenantId) {
        super(systemContext);
        this.tenantId = tenantId;
    }

    @Override
    protected boolean doProcess(JnksIotActorMsg msg) {
        if (msg instanceof ToCalculatedFieldSystemMsg cfm) {
            Exception cause;
            try {
                return doProcessCfMsg(cfm);
            } catch (CalculatedFieldException cfe) {
                if (DebugModeUtil.isDebugFailuresAvailable(cfe.getCtx().getCalculatedField())) {
                    String message;
                    if (cfe.getErrorMessage() != null) {
                        message = cfe.getErrorMessage();
                    } else if (cfe.getCause() != null) {
                        message = cfe.getCause().getMessage();
                    } else {
                        message = "N/A";
                    }
                    systemContext.persistCalculatedFieldDebugEvent(tenantId, cfe.getCtx().getCfId(), cfe.getEventEntity(), cfe.getArguments(), cfe.getMsgId(), cfe.getMsgType(), null, message);
                }
                cause = cfe.getCause();
            } catch (Exception e) {
                logProcessingException(e);
                cause = e;
            }
            cfm.getCallback().onFailure(cause);
            return true;
        } else {
            return false;
        }
    }

    abstract void logProcessingException(Exception e);

    abstract boolean doProcessCfMsg(ToCalculatedFieldSystemMsg msg) throws CalculatedFieldException;

}
