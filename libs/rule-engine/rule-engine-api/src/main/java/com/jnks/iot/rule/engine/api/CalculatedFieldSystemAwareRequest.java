package com.jnks.iot.rule.engine.api;

import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.msg.TbMsgType;

import java.util.List;
import java.util.UUID;

public interface CalculatedFieldSystemAwareRequest {

    List<CalculatedFieldId> getPreviousCalculatedFieldIds();

    UUID getTbMsgId();

    TbMsgType getTbMsgType();

}
