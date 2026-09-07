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
package org.thingsboard.server.service.edqs;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.edqs.EdqsConfig;
import org.thingsboard.server.queue.kafka.TbKafkaAdmin;
import org.thingsboard.server.queue.kafka.TbKafkaSettings;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Kafka 队列场景下的 {@link EdqsSyncService} 实现。
 * <p>
 * 在 <b>Bean 构造阶段</b>用 Kafka Admin 检查配置的 EDQS <b>events Topic</b>
 *（{@link EdqsConfig#getEventsTopic()}）在 {@link EdqsConfig#getPartitions()}
 * 个分区上是否<strong>全部为空</strong>：
 * <ul>
 *   <li>全空 → {@link #isSyncNeeded()} 为 {@code true}，启动时倾向做一次 DB → events 全量灌数；</li>
 *   <li>任一分区已有数据 → 视为历史上已同步或已有增量，{@code false}，
 *       避免每次重启 Core 都把全库再推一遍。</li>
 * </ul>
 * <p>
 * 判定结果缓存在 {@link #syncNeeded}，进程生命周期内不变（不会在运行中重新探测 topic）。
 * 是否真正调用 {@link #sync()} 仍由 {@link DefaultEdqsService} 结合系统属性
 * {@code edqsSyncState} 决定：例如 topic 非空但状态未 FINISHED，仍可能再 sync。
 * <p>
 * <b>生效条件：</b>{@code queue.edqs.sync.enabled=true} 且 {@code queue.type=kafka}
 *（含微服务 remote、以及 monolith/core 的 local + Kafka）。
 * 与 {@link LocalEdqsSyncService} 互斥，同一进程只会激活其一。
 * <p>
 * 全量扫描与事件写出逻辑全部继承自 {@link EdqsSyncService}；本类只提供
 * 「要不要灌」的介质判断。
 *
 * @see EdqsSyncService
 * @see LocalEdqsSyncService
 * @see DefaultEdqsService
 */
@Service
@ConditionalOnExpression("'${queue.edqs.sync.enabled:true}' == 'true' && '${queue.type:null}' == 'kafka'")
public class KafkaEdqsSyncService extends EdqsSyncService {

    /**
     * 构造时一次性探测并缓存：events Topic 全部分区是否为空。
     * {@code true} 表示建议执行全量同步。
     */
    private final boolean syncNeeded;

    /**
     * 创建临时 {@link TbKafkaAdmin}（无额外 topic 配置 map），对
     * {@code eventsTopic-0 .. eventsTopic-(partitions-1)} 调用
     * {@link TbKafkaAdmin#areAllTopicsEmpty}。
     * <p>
     * Topic 全名通过 {@link TopicPartitionInfo} 拼出，与生产/消费侧命名一致
     *（含全局 {@code queue.prefix} 等规则时由 TPI 处理）。
     *
     * @param kafkaSettings Kafka 连接配置
     * @param edqsConfig    EDQS topic 名与分区数
     */
    public KafkaEdqsSyncService(TbKafkaSettings kafkaSettings, EdqsConfig edqsConfig) {
        TbKafkaAdmin kafkaAdmin = new TbKafkaAdmin(kafkaSettings, Collections.emptyMap());
        this.syncNeeded = kafkaAdmin.areAllTopicsEmpty(IntStream.range(0, edqsConfig.getPartitions())
                .mapToObj(partition -> TopicPartitionInfo.builder()
                        .topic(edqsConfig.getEventsTopic())
                        .partition(partition)
                        .build().getFullTopicName())
                .collect(Collectors.toSet()));
    }

    /**
     * 返回构造时缓存的判定结果。
     *
     * @return events Topic 全空则为 {@code true}
     */
    @Override
    public boolean isSyncNeeded() {
        return syncNeeded;
    }

}
