package com.jnks.iot.server.service.queue.processing;

import lombok.Getter;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

import java.util.UUID;

public class IdMsgPair<T extends com.google.protobuf.GeneratedMessageV3> {
    @Getter
    final UUID uuid;
    @Getter
    final JnksIotProtoQueueMsg<T> msg;

    public IdMsgPair(UUID uuid, JnksIotProtoQueueMsg<T> msg) {
        this.uuid = uuid;
        this.msg = msg;
    }
}
