package com.jnks.iot.server.service.queue.processing;

import lombok.Getter;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;

import java.util.UUID;

public class IdMsgPair<T extends com.google.protobuf.GeneratedMessageV3> {
    @Getter
    final UUID uuid;
    @Getter
    final TbProtoQueueMsg<T> msg;

    public IdMsgPair(UUID uuid, TbProtoQueueMsg<T> msg) {
        this.uuid = uuid;
        this.msg = msg;
    }
}
