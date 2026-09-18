package org.thingsboard.rule.engine.api;

import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.msg.TbMsgType;

import java.util.List;
import java.util.UUID;

public interface CalculatedFieldSystemAwareRequest {

    List<CalculatedFieldId> getPreviousCalculatedFieldIds();

    UUID getTbMsgId();

    TbMsgType getTbMsgType();

}
