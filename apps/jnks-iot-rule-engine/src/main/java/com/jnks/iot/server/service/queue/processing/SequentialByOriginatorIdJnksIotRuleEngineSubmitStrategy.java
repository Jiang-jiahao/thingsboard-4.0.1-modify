package com.jnks.iot.server.service.queue.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.msg.gen.MsgProtos;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.UUID;

@Slf4j
public class SequentialByOriginatorIdJnksIotRuleEngineSubmitStrategy extends SequentialByEntityIdJnksIotRuleEngineSubmitStrategy {

    public SequentialByOriginatorIdJnksIotRuleEngineSubmitStrategy(String queueName) {
        super(queueName);
    }

    @Override
    protected EntityId getEntityId(TransportProtos.ToRuleEngineMsg msg) {
        try {
            MsgProtos.JnksIotMsgProto proto = MsgProtos.JnksIotMsgProto.parseFrom(msg.getJnksIotMsg());
            return EntityIdFactory.getByTypeAndUuid(proto.getEntityType(), new UUID(proto.getEntityIdMSB(), proto.getEntityIdLSB()));
        } catch (InvalidProtocolBufferException e) {
            log.warn("[{}] Failed to parse JnksIotMsg: {}", queueName, msg);
            return null;
        }
    }
}
