package com.jnks.iot.server.service.queue.processing;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

@Slf4j
public class BatchJnksIotRuleEngineSubmitStrategy extends AbstractJnksIotRuleEngineSubmitStrategy {

    private final int batchSize;
    private final AtomicInteger packIdx = new AtomicInteger(0);
    private final Map<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> pendingPack = new LinkedHashMap<>();
    private volatile BiConsumer<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgConsumer;

    public BatchJnksIotRuleEngineSubmitStrategy(String queueName, int batchSize) {
        super(queueName);
        this.batchSize = batchSize;
    }

    @Override
    public void submitAttempt(BiConsumer<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgConsumer) {
        this.msgConsumer = msgConsumer;
        submitNext();
    }

    @Override
    public void update(ConcurrentMap<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> reprocessMap) {
        super.update(reprocessMap);
        packIdx.set(0);
    }

    @Override
    protected void doOnSuccess(UUID id) {
        boolean endOfPendingPack;
        synchronized (pendingPack) {
            JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg> msg = pendingPack.remove(id);
            endOfPendingPack = msg != null && pendingPack.isEmpty();
        }
        if (endOfPendingPack) {
            packIdx.incrementAndGet();
            submitNext();
        }
    }

    private void submitNext() {
        int listSize = orderedMsgList.size();
        int startIdx = Math.min(packIdx.get() * batchSize, listSize);
        int endIdx = Math.min(startIdx + batchSize, listSize);
        Map<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> tmpPack;
        synchronized (pendingPack) {
            pendingPack.clear();
            for (int i = startIdx; i < endIdx; i++) {
                IdMsgPair<TransportProtos.ToRuleEngineMsg> pair = orderedMsgList.get(i);
                pendingPack.put(pair.uuid, pair.msg);
            }
            tmpPack = new LinkedHashMap<>(pendingPack);
        }
        int submitSize = pendingPack.size();
        if (log.isDebugEnabled() && submitSize > 0) {
            log.debug("[{}] submitting [{}] messages to rule engine", queueName, submitSize);
        }
        tmpPack.forEach(msgConsumer);
    }

}
