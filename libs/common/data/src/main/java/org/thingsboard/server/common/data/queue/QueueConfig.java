package org.thingsboard.server.common.data.queue;

import lombok.Data;

/**
 * 队列运行时配置接口。
 * 目前只关注两件事：
 *   1. 是否“每个分区独占一条消费线程”
 *   2. 轮询（poll）Kafka 的间隔时间
 */
public interface QueueConfig {

    /**
     * true → 一个分区一个消费者
     * false → 所有分区一个消费者
     * @return 是否每个分区一个消费者
     */
    boolean isConsumerPerPartition();

    /**
     * 每次 poll 的超时/间隔时间，单位毫秒
     * @return 每次 poll 的超时/间隔时间
     */
    int getPollInterval();

    /**
     * 快速创建一个配置实例
     * @param consumerPerPartition 是否每个分区一个消费者
     * @param pollInterval 每次 poll 的超时/间隔时间
     * @return 配置实例
     */
    static QueueConfig of(boolean consumerPerPartition, long pollInterval) {
        return new BasicQueueConfig(consumerPerPartition, (int) pollInterval);
    }

    /**
     * 队列运行时配置接口最小实现类
     */
    @Data
    class BasicQueueConfig implements QueueConfig {
        private final boolean consumerPerPartition;
        private final int pollInterval;
    }

}
