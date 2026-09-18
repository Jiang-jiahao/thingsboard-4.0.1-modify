package org.thingsboard.server.service.cf;

import org.thingsboard.server.common.msg.queue.TbCallback;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.exception.CalculatedFieldStateException;
import org.thingsboard.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.common.consumer.PartitionedQueueConsumerManager;
import org.thingsboard.server.queue.discovery.QueueKey;
import org.thingsboard.server.service.cf.ctx.CalculatedFieldEntityCtxId;
import org.thingsboard.server.service.cf.ctx.state.CalculatedFieldState;

import java.util.Set;

/**
 * 专注于计算字段状态的持久化管理
 */
public interface CalculatedFieldStateService {

    void init(PartitionedQueueConsumerManager<TbProtoQueueMsg<ToCalculatedFieldMsg>> eventConsumer);

    void persistState(CalculatedFieldEntityCtxId stateId, CalculatedFieldState state, TbCallback callback) throws CalculatedFieldStateException;

    void removeState(CalculatedFieldEntityCtxId stateId, TbCallback callback);

    void restore(QueueKey queueKey, Set<TopicPartitionInfo> partitions);

    void delete(Set<TopicPartitionInfo> partitions);

    Set<TopicPartitionInfo> getPartitions();

    void stop();

}
