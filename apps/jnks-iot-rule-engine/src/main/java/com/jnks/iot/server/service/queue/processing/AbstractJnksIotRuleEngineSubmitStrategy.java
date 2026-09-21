package com.jnks.iot.server.service.queue.processing;

import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public abstract class AbstractJnksIotRuleEngineSubmitStrategy implements JnksIotRuleEngineSubmitStrategy {

    protected final String queueName;
    protected List<IdMsgPair<TransportProtos.ToRuleEngineMsg>> orderedMsgList;
    private volatile boolean stopped;

    public AbstractJnksIotRuleEngineSubmitStrategy(String queueName) {
        this.queueName = queueName;
    }

    protected abstract void doOnSuccess(UUID id);

    @Override
    public void init(List<JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> msgs) {
        orderedMsgList = msgs.stream().map(msg -> new IdMsgPair<>(UUID.randomUUID(), msg)).collect(Collectors.toList());
    }

    @Override
    public ConcurrentMap<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> getPendingMap() {
        return orderedMsgList.stream().collect(Collectors.toConcurrentMap(pair -> pair.uuid, pair -> pair.msg));
    }

    @Override
    public void update(ConcurrentMap<UUID, JnksIotProtoQueueMsg<TransportProtos.ToRuleEngineMsg>> reprocessMap) {
        List<IdMsgPair<TransportProtos.ToRuleEngineMsg>> newOrderedMsgList = new ArrayList<>(reprocessMap.size());
        for (IdMsgPair<TransportProtos.ToRuleEngineMsg> pair : orderedMsgList) {
            if (reprocessMap.containsKey(pair.uuid)) {
                if (StringUtils.isNotEmpty(pair.getMsg().getValue().getFailureMessage())) {
                    var toRuleEngineMsg = TransportProtos.ToRuleEngineMsg.newBuilder(pair.getMsg().getValue())
                            .clearFailureMessage()
                            .clearRelationTypes()
                            .build();
                    var newMsg = new JnksIotProtoQueueMsg<>(pair.getMsg().getKey(), toRuleEngineMsg, pair.getMsg().getHeaders());
                    newOrderedMsgList.add(new IdMsgPair<>(pair.getUuid(), newMsg));
                } else {
                    newOrderedMsgList.add(pair);
                }
            }
        }
        orderedMsgList = newOrderedMsgList;
    }

    @Override
    public void onSuccess(UUID id) {
        if (!stopped) {
            doOnSuccess(id);
        }
    }

    @Override
    public void stop() {
        stopped = true;
    }
}
