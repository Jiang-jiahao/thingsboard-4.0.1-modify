package org.thingsboard.server.actors.calculatedField;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.DebugModeUtil;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.actors.service.ContextAwareActor;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.TbActorMsg;
import org.thingsboard.server.common.msg.ToCalculatedFieldSystemMsg;

@Slf4j
public abstract class AbstractCalculatedFieldActor extends ContextAwareActor {

    protected final TenantId tenantId;

    public AbstractCalculatedFieldActor(ActorSystemContext systemContext, TenantId tenantId) {
        super(systemContext);
        this.tenantId = tenantId;
    }

    @Override
    protected boolean doProcess(TbActorMsg msg) {
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
