/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.queue.kafka;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.server.common.data.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 消费组滞后（lag）统计与日志打印服务。
 * <p>
 * 仅在 {@code queue.type=kafka} 时注册。本身不参与业务消息的 poll / 处理 / commit，
 * 只周期性观察已注册消费组的「已提交位点」与「分区末尾位点」之差，发现积压时打 info 日志，
 * 便于运维排查消费跟不上生产的问题。
 * <p>
 * <b>工作方式：</b>
 * <ol>
 *   <li>{@link TbKafkaConsumerTemplate} 创建时调用 {@link #registerClientGroup}，
 *       销毁时 {@link #unregisterClientGroup}；</li>
 *   <li>本服务维护待监控的 {@link #monitoredGroups}；</li>
 *   <li>按 {@link TbKafkaConsumerStatisticConfig#getPrintIntervalMs()} 定时：
 *       用 AdminClient 拉消费组 committed offset，再用内部只读 {@link #consumer}
 *       查对应分区的 end offset，计算 lag；</li>
 *   <li>仅输出 lag ≠ 0 的分区，无积压则静默。</li>
 * </ol>
 * <p>
 * 若统计配置 {@code enabled=false}，{@link #init} 直接返回，注册/注销也成为空操作。
 *
 * @see TbKafkaConsumerTemplate
 * @see TbKafkaConsumerStatisticConfig
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "queue", value = "type", havingValue = "kafka")
public class TbKafkaConsumerStatsService {

    /**
     * 当前需要监控 lag 的 Kafka consumer group id 集合。
     * <p>
     * 由业务消费者在生命周期内注册/注销；定时任务遍历本集合逐组统计。
     */
    private final Set<String> monitoredGroups = ConcurrentHashMap.newKeySet();

    /**
     * Kafka 连接与 AdminClient 等共享设置。
     */
    private final TbKafkaSettings kafkaSettings;

    /**
     * 统计开关、打印间隔、Admin/endOffsets 调用超时等配置。
     */
    private final TbKafkaConsumerStatisticConfig statsConfig;

    /**
     * 仅用于查询分区 end offset 的只读 KafkaConsumer。
     * <p>
     * 不订阅业务 Topic、不提交 offset；client/group id 固定为 stats-loader，
     * 避免与真实业务消费组冲突。统计未启用时为 {@code null}。
     */
    private Consumer<String, byte[]> consumer;

    /**
     * 定时打印 lag 的单线程调度器；统计未启用时为 {@code null}。
     */
    private ScheduledExecutorService statsPrintScheduler;

    /**
     * 统计启用时创建调度器与只读 consumer，并启动周期性打日志任务。
     * <p>
     * {@code enabled=false} 时不做任何初始化，后续 register/unregister 亦因配置判断而无效。
     */
    @PostConstruct
    public void init() {
        if (!statsConfig.getEnabled()) {
            return;
        }
        this.statsPrintScheduler = ThingsBoardExecutors.newSingleThreadScheduledExecutor("kafka-consumer-stats");

        Properties consumerProps = kafkaSettings.toConsumerProps(null);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-stats-loader-client");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-stats-loader-client-group");
        this.consumer = new KafkaConsumer<>(consumerProps);

        startLogScheduling();
    }

    /**
     * 按固定间隔遍历 {@link #monitoredGroups}，计算并打印存在 lag 的分区。
     * <p>
     * 对每个 group：
     * <ol>
     *   <li>{@code listConsumerGroupOffsets} 得到各分区 committed offset；</li>
     *   <li>{@code consumer.endOffsets} 得到同批分区的最新 offset（end）；</li>
     *   <li>{@link #getTopicsStatsWithLag} 过滤出 lag ≠ 0 的项并打 info。</li>
     * </ol>
     * Admin / endOffsets 均受 {@link TbKafkaConsumerStatisticConfig#getKafkaResponseTimeoutMs()} 约束；
     * 单组失败只 warn，不影响其它组。若当前日志级别未开 info，整轮跳过（见 {@link #isStatsPrintRequired}）。
     */
    private void startLogScheduling() {
        Duration timeoutDuration = Duration.ofMillis(statsConfig.getKafkaResponseTimeoutMs());
        statsPrintScheduler.scheduleWithFixedDelay(() -> {
            if (!isStatsPrintRequired()) {
                return;
            }
            for (String groupId : monitoredGroups) {
                try {
                    Map<TopicPartition, OffsetAndMetadata> groupOffsets = kafkaSettings.getAdminClient().listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                            .get(statsConfig.getKafkaResponseTimeoutMs(), TimeUnit.MILLISECONDS);
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(groupOffsets.keySet(), timeoutDuration);

                    List<GroupTopicStats> lagTopicsStats = getTopicsStatsWithLag(groupOffsets, endOffsets);
                    if (!lagTopicsStats.isEmpty()) {
                        StringBuilder builder = new StringBuilder();
                        for (int i = 0; i < lagTopicsStats.size(); i++) {
                            builder.append(lagTopicsStats.get(i).toString());
                            if (i != lagTopicsStats.size() - 1) {
                                builder.append(", ");
                            }
                        }
                        log.info("[{}] Topic partitions with lag: [{}].", groupId, builder.toString());
                    }
                } catch (Exception e) {
                    log.warn("[{}] Failed to get consumer group stats. Reason - {}.", groupId, e.getMessage());
                    log.trace("Detailed error: ", e);
                }
            }

        }, statsConfig.getPrintIntervalMs(), statsConfig.getPrintIntervalMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * 是否值得执行本轮统计打印。
     * <p>
     * 当前仅当 info 日志开启时返回 true，避免在关闭 info 时仍频繁打 Admin API。
     *
     * @return info 启用则为 true
     */
    private boolean isStatsPrintRequired() {
        return log.isInfoEnabled();
    }

    /**
     * 对比 committed 与 end offset，只保留存在积压（lag ≠ 0）的分区统计。
     * <p>
     * {@code lag = endOffset - committedOffset}。lag 为 0 表示该分区已追上，不进入结果列表。
     *
     * @param groupOffsets 消费组已提交位点
     * @param endOffsets   对应分区的 log end offset
     * @return 有 lag 的分区统计列表；全部追上时为空列表
     */
    private List<GroupTopicStats> getTopicsStatsWithLag(Map<TopicPartition, OffsetAndMetadata> groupOffsets, Map<TopicPartition, Long> endOffsets) {
        List<GroupTopicStats> consumerGroupStats = new ArrayList<>();
        for (TopicPartition topicPartition : groupOffsets.keySet()) {
            long endOffset = endOffsets.get(topicPartition);
            long committedOffset = groupOffsets.get(topicPartition).offset();
            long lag = endOffset - committedOffset;
            if (lag != 0) {
                GroupTopicStats groupTopicStats = GroupTopicStats.builder()
                        .topic(topicPartition.topic())
                        .partition(topicPartition.partition())
                        .committedOffset(committedOffset)
                        .endOffset(endOffset)
                        .lag(lag)
                        .build();
                consumerGroupStats.add(groupTopicStats);
            }
        }
        return consumerGroupStats;
    }

    /**
     * 将业务消费组加入监控集合。
     * <p>
     * 通常由 {@link TbKafkaConsumerTemplate} 在构造时调用。统计未启用或 {@code groupId} 为空时忽略。
     *
     * @param groupId Kafka consumer group id
     */
    public void registerClientGroup(String groupId) {
        if (statsConfig.getEnabled() && !StringUtils.isEmpty(groupId)) {
            monitoredGroups.add(groupId);
        }
    }

    /**
     * 将业务消费组移出监控集合。
     * <p>
     * 通常在消费者关闭 / unsubscribe 时调用，避免继续查询已不存在的 group。
     *
     * @param groupId Kafka consumer group id
     */
    public void unregisterClientGroup(String groupId) {
        if (statsConfig.getEnabled() && !StringUtils.isEmpty(groupId)) {
            monitoredGroups.remove(groupId);
        }
    }

    /**
     * 关闭调度器与只读 consumer，释放 Kafka 客户端资源。
     */
    @PreDestroy
    public void destroy() {
        if (statsPrintScheduler != null) {
            statsPrintScheduler.shutdownNow();
        }
        if (consumer != null) {
            consumer.close();
        }
    }


    /**
     * 单个 topic-partition 上的 lag 快照，仅用于日志输出。
     */
    @Builder
    @Data
    private static class GroupTopicStats {
        /** Topic 名 */
        private String topic;
        /** 分区号 */
        private int partition;
        /** 消费组已提交 offset */
        private long committedOffset;
        /** 分区当前 end offset（最新消息之后的位置） */
        private long endOffset;
        /** {@code endOffset - committedOffset}，大于 0 表示积压 */
        private long lag;

        @Override
        public String toString() {
            return "[" +
                    "topic=[" + topic + ']' +
                    ", partition=[" + partition + "]" +
                    ", committedOffset=[" + committedOffset + "]" +
                    ", endOffset=[" + endOffset + "]" +
                    ", lag=[" + lag + "]" +
                    "]";
        }
    }
}
