package com.jnks.iot.server.service.cf.ctx;

import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.calculatedField.CalculatedFieldStateRestoreMsg;
import com.jnks.iot.server.common.msg.queue.TbCallback;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.exception.CalculatedFieldStateException;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldStateProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;
import com.jnks.iot.server.queue.common.state.QueueStateService;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.service.cf.CalculatedFieldStateService;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldState;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jnks.iot.server.utils.CalculatedFieldUtils.fromProto;
import static com.jnks.iot.server.utils.CalculatedFieldUtils.toProto;

public abstract class AbstractCalculatedFieldStateService implements CalculatedFieldStateService {

    @Autowired
    private ActorSystemContext actorSystemContext;

    protected QueueStateService<TbProtoQueueMsg<ToCalculatedFieldMsg>, TbProtoQueueMsg<CalculatedFieldStateProto>> stateService;

    @Override
    public final void persistState(CalculatedFieldEntityCtxId stateId, CalculatedFieldState state, TbCallback callback) {
        if (state.isSizeExceedsLimit()) {
            throw new CalculatedFieldStateException("State size exceeds the maximum allowed limit. The state will not be persisted to RocksDB.");
        }
        doPersist(stateId, toProto(stateId, state), callback);
    }

    protected abstract void doPersist(CalculatedFieldEntityCtxId stateId, CalculatedFieldStateProto stateMsgProto, TbCallback callback);

    @Override
    public final void removeState(CalculatedFieldEntityCtxId stateId, TbCallback callback) {
        doRemove(stateId, callback);
    }

    protected abstract void doRemove(CalculatedFieldEntityCtxId stateId, TbCallback callback);

    protected void processRestoredState(CalculatedFieldStateProto stateMsg) {
        var id = fromProto(stateMsg.getId());
        var state = fromProto(stateMsg);
        processRestoredState(id, state);
    }

    protected void processRestoredState(CalculatedFieldEntityCtxId id, CalculatedFieldState state) {
        actorSystemContext.tell(new CalculatedFieldStateRestoreMsg(id, state));
    }

    @Override
    public void restore(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        stateService.update(queueKey, partitions);
    }

    @Override
    public void delete(Set<TopicPartitionInfo> partitions) {
        stateService.delete(partitions);
    }

    @Override
    public Set<TopicPartitionInfo> getPartitions() {
        return stateService.getPartitions().values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public void stop() {
        stateService.stop();
    }

}
