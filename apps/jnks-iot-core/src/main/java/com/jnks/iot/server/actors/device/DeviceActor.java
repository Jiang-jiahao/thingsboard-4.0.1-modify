package com.jnks.iot.server.actors.device;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.JnksIotActorCtx;
import com.jnks.iot.server.actors.JnksIotActorException;
import com.jnks.iot.server.actors.service.ContextAwareActor;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.rpc.FromDeviceRpcResponseActorMsg;
import com.jnks.iot.server.common.msg.rpc.RemoveRpcActorMsg;
import com.jnks.iot.server.common.msg.rpc.ToDeviceRpcRequestActorMsg;
import com.jnks.iot.server.common.msg.rule.engine.DeviceAttributesEventNotificationMsg;
import com.jnks.iot.server.common.msg.rule.engine.DeviceNameOrTypeUpdateMsg;
import com.jnks.iot.server.common.msg.timeout.DeviceActorServerSideRpcTimeoutMsg;
import com.jnks.iot.server.service.transport.msg.TransportToDeviceActorMsgWrapper;

@Slf4j
public class DeviceActor extends ContextAwareActor {

    private final DeviceActorMessageProcessor processor;

    DeviceActor(ActorSystemContext systemContext, TenantId tenantId, DeviceId deviceId) {
        super(systemContext);
        this.processor = new DeviceActorMessageProcessor(systemContext, tenantId, deviceId);
    }

    @Override
    public void init(JnksIotActorCtx ctx) throws JnksIotActorException {
        super.init(ctx);
        log.debug("[{}][{}] Starting device actor.", processor.tenantId, processor.deviceId);
        try {
            processor.init(ctx);
            log.debug("[{}][{}] Device actor started.", processor.tenantId, processor.deviceId);
        } catch (Exception e) {
            log.warn("[{}][{}] Unknown failure", processor.tenantId, processor.deviceId, e);
            throw new JnksIotActorException("Failed to initialize device actor", e);
        }
    }

    @Override
    protected boolean doProcess(JnksIotActorMsg msg) {
        switch (msg.getMsgType()) {
            case TRANSPORT_TO_DEVICE_ACTOR_MSG:
                processor.process((TransportToDeviceActorMsgWrapper) msg);
                break;
            case DEVICE_ATTRIBUTES_UPDATE_TO_DEVICE_ACTOR_MSG:
                processor.processAttributesUpdate((DeviceAttributesEventNotificationMsg) msg);
                break;
            case DEVICE_DELETE_TO_DEVICE_ACTOR_MSG:
                ctx.stop(ctx.getSelf());
                break;
            case DEVICE_CREDENTIALS_UPDATE_TO_DEVICE_ACTOR_MSG:
                processor.processCredentialsUpdate(msg);
                break;
            case DEVICE_NAME_OR_TYPE_UPDATE_TO_DEVICE_ACTOR_MSG:
                processor.processNameOrTypeUpdate((DeviceNameOrTypeUpdateMsg) msg);
                break;
            case DEVICE_RPC_REQUEST_TO_DEVICE_ACTOR_MSG:
                processor.processRpcRequest(ctx, (ToDeviceRpcRequestActorMsg) msg);
                break;
            case DEVICE_RPC_RESPONSE_TO_DEVICE_ACTOR_MSG:
                processor.processRpcResponse((FromDeviceRpcResponseActorMsg) msg);
                break;
            case DEVICE_ACTOR_SERVER_SIDE_RPC_TIMEOUT_MSG:
                processor.processServerSideRpcTimeout((DeviceActorServerSideRpcTimeoutMsg) msg);
                break;
            case SESSION_TIMEOUT_MSG:
                processor.checkSessionsTimeout();
                break;
            case REMOVE_RPC_TO_DEVICE_ACTOR_MSG:
                processor.processRemoveRpc((RemoveRpcActorMsg) msg);
                break;
            default:
                return false;
        }
        return true;
    }

}
