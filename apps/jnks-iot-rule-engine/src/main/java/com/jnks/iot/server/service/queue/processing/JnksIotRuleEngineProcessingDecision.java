package com.jnks.iot.server.service.queue.processing;

import lombok.Data;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@Data
public class JnksIotRuleEngineProcessingDecision {

    private final boolean commit;
    private final ConcurrentMap<UUID, JnksIotProtoQueueMsg<ToRuleEngineMsg>> reprocessMap;

}
