package com.jnks.iot.server.service.queue.processing;

import lombok.Data;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@Data
public class TbRuleEngineProcessingDecision {

    private final boolean commit;
    private final ConcurrentMap<UUID, TbProtoQueueMsg<ToRuleEngineMsg>> reprocessMap;

}
