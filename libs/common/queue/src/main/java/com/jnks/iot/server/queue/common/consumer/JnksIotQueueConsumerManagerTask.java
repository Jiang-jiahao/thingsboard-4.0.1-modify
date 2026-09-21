package com.jnks.iot.server.queue.common.consumer;

import com.jnks.iot.server.common.data.queue.QueueConfig;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public interface JnksIotQueueConsumerManagerTask {

    QueueTaskType getType();

    record DeleteQueueTask(boolean drainQueue) implements JnksIotQueueConsumerManagerTask {
        @Override
        public QueueTaskType getType() {
            return QueueTaskType.DELETE;
        }
    }

    record UpdateConfigTask(QueueConfig config) implements JnksIotQueueConsumerManagerTask {
        @Override
        public QueueTaskType getType() {
            return QueueTaskType.UPDATE_CONFIG;
        }
    }

    record UpdatePartitionsTask(Set<TopicPartitionInfo> partitions) implements JnksIotQueueConsumerManagerTask {
        @Override
        public QueueTaskType getType() {
            return QueueTaskType.UPDATE_PARTITIONS;
        }
    }

    record AddPartitionsTask(Set<TopicPartitionInfo> partitions,
                             Consumer<TopicPartitionInfo> onStop,
                             Function<String, Long> startOffsetProvider) implements JnksIotQueueConsumerManagerTask {
        @Override
        public QueueTaskType getType() {
            return QueueTaskType.ADD_PARTITIONS;
        }
    }

    record RemovePartitionsTask(Set<TopicPartitionInfo> partitions) implements JnksIotQueueConsumerManagerTask {
        @Override
        public QueueTaskType getType() {
            return QueueTaskType.REMOVE_PARTITIONS;
        }
    }

    record DeletePartitionsTask(Set<TopicPartitionInfo> partitions) implements JnksIotQueueConsumerManagerTask {
        @Override
        public QueueTaskType getType() {
            return QueueTaskType.REMOVE_PARTITIONS;
        }
    }

}
