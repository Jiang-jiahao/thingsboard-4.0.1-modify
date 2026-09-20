package com.jnks.iot.server.service.cf.ctx.state;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.msg.queue.TbCallback;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldStateProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;
import com.jnks.iot.server.queue.common.consumer.PartitionedQueueConsumerManager;
import com.jnks.iot.server.queue.common.state.DefaultQueueStateService;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.service.cf.ctx.AbstractCalculatedFieldStateService;
import com.jnks.iot.server.service.cf.CfRocksDb;
import com.jnks.iot.server.service.cf.ctx.CalculatedFieldEntityCtxId;

import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnExpression("'${queue.type:null}'=='in-memory'")
public class RocksDBCalculatedFieldStateService extends AbstractCalculatedFieldStateService {

    private final CfRocksDb cfRocksDb;

    @Override
    public void init(PartitionedQueueConsumerManager<TbProtoQueueMsg<ToCalculatedFieldMsg>> eventConsumer) {
        super.stateService = new DefaultQueueStateService<>(eventConsumer);
    }

    @Override
    protected void doPersist(CalculatedFieldEntityCtxId stateId, CalculatedFieldStateProto stateMsgProto, TbCallback callback) {
        cfRocksDb.put(stateId.toKey(), stateMsgProto.toByteArray());
        callback.onSuccess();
    }

    @Override
    protected void doRemove(CalculatedFieldEntityCtxId stateId, TbCallback callback) {
        cfRocksDb.delete(stateId.toKey());
        callback.onSuccess();
    }

    @Override
    public void restore(QueueKey queueKey, Set<TopicPartitionInfo> partitions) {
        if (stateService.getPartitions().isEmpty()) {
            cfRocksDb.forEach((key, value) -> {
                try {
                    processRestoredState(CalculatedFieldStateProto.parseFrom(value));
                } catch (InvalidProtocolBufferException e) {
                    log.error("[{}] Failed to process restored state", key, e);
                }
            });
        }
        super.restore(queueKey, partitions);
    }

}
