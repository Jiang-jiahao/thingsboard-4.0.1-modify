package com.jnks.iot.server.service.cf;

import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.exception.CalculatedFieldStateException;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.common.consumer.PartitionedQueueConsumerManager;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.service.cf.ctx.CalculatedFieldEntityCtxId;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldState;

import java.util.Set;

/**
 * 专注于计算字段状态的持久化管理
 */
public interface CalculatedFieldStateService {

    void init(PartitionedQueueConsumerManager<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> eventConsumer);

    void persistState(CalculatedFieldEntityCtxId stateId, CalculatedFieldState state, JnksIotCallback callback) throws CalculatedFieldStateException;

    void removeState(CalculatedFieldEntityCtxId stateId, JnksIotCallback callback);

    void restore(QueueKey queueKey, Set<TopicPartitionInfo> partitions);

    void delete(Set<TopicPartitionInfo> partitions);

    Set<TopicPartitionInfo> getPartitions();

    void stop();

}
