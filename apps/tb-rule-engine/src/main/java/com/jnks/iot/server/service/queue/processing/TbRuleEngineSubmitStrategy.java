package com.jnks.iot.server.service.queue.processing;

import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * 规则引擎消息提交策略的接口
 */
public interface TbRuleEngineSubmitStrategy {

    void init(List<TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgs);

    ConcurrentMap<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> getPendingMap();

    void submitAttempt(BiConsumer<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgConsumer);

    void update(ConcurrentMap<UUID, TbProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> reprocessMap);

    void onSuccess(UUID id);

    void stop();
}
