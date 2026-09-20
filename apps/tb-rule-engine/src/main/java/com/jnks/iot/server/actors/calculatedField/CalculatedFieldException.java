package com.jnks.iot.server.actors.calculatedField;

import lombok.Builder;
import lombok.Getter;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.msg.TbMsgType;
import com.jnks.iot.server.service.cf.ctx.state.ArgumentEntry;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldCtx;

import java.util.Map;
import java.util.UUID;

@Getter
@Builder
public class CalculatedFieldException extends Exception {

    private final CalculatedFieldCtx ctx;
    private final EntityId eventEntity;
    private final UUID msgId;
    private final TbMsgType msgType;
    private Map<String, ArgumentEntry> arguments;
    private String errorMessage;
    private Exception cause;

}
