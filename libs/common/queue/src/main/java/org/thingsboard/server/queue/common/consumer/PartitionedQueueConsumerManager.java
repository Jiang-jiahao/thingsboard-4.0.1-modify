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
package org.thingsboard.server.queue.common.consumer;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.queue.QueueConfig;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.common.consumer.TbQueueConsumerManagerTask.AddPartitionsTask;
import org.thingsboard.server.queue.common.consumer.TbQueueConsumerManagerTask.DeletePartitionsTask;
import org.thingsboard.server.queue.common.consumer.TbQueueConsumerManagerTask.RemovePartitionsTask;
import org.thingsboard.server.queue.discovery.QueueKey;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 分区级队列消费者管理器：在 {@link MainQueueConsumerManager} 之上固定采用
 * 「每个分区一个独立消费者」模式，并提供增量增删分区、删除底层 Topic 的能力。
 * <p>
 * 与父类全量 {@code update(partitions)} 不同，本类面向需要细粒度控制分区生命周期的场景
 * （例如按租户/实体动态创建隔离 Topic、消费结束后删除 Topic 等）。分区变更通过专用任务
 * {@link AddPartitionsTask} / {@link RemovePartitionsTask} / {@link DeletePartitionsTask}
 * 提交，在父类 {@link MainQueueConsumerManager#processTask} 钩子中<strong>立即处理</strong>，
 * 不会像配置更新、全量分区更新那样在本轮任务末尾合并，因此响应更及时。
 * <p>
 * <b>与父类的关系：</b>
 * <ul>
 *   <li>构造时强制 {@code QueueConfig.of(true, pollInterval)}，即始终
 *       {@code consumerPerPartition = true}；</li>
 *   <li>持有父类 {@link MainQueueConsumerManager.ConsumerPerPartitionWrapper} 的强类型引用，
 *       以便直接调用 {@code addPartitions} / {@code removePartitions}；</li>
 *   <li>依赖 {@link TbQueueAdmin} 在「删除分区」任务中物理删除 Topic。</li>
 * </ul>
 * <p>
 * <b>典型调用路径：</b>
 * {@link #addPartitions} / {@link #removePartitions} / {@link #delete} →
 * {@code addTask} → 任务线程持锁 → {@link #processTask} → 包装器增删消费者（删除时再调
 * {@link TbQueueAdmin#deleteTopic}）。
 *
 * @param <M> 队列消息类型，需实现 {@link TbQueueMsg}
 * @see MainQueueConsumerManager
 * @see TbQueueConsumerManagerTask
 */
@Slf4j
public class PartitionedQueueConsumerManager<M extends TbQueueMsg> extends MainQueueConsumerManager<M, QueueConfig> {

    /**
     * 父类「每分区一消费者」包装器的强类型引用。
     * <p>
     * 构造时从 {@code super.consumerWrapper} 转型得到，供增量增删分区时直接调用
     * {@code addPartitions} / {@code removePartitions}，避免再次走全量 diff。
     */
    private final ConsumerPerPartitionWrapper consumerWrapper;

    /**
     * 队列管理客户端，用于删除 Topic 等运维操作。
     * <p>
     * 仅在处理 {@link DeletePartitionsTask} 时使用：先停止对应消费者，再按分区的
     * full topic name 调用 {@link TbQueueAdmin#deleteTopic}。
     */
    private final TbQueueAdmin queueAdmin;

    /**
     * 本管理器关联的逻辑 Topic 名（不含分区后缀等完整路由信息时的基础名）。
     * <p>
     * 便于上层按 Topic 维度检索或展示；实际订阅/删除仍以各
     * {@link TopicPartitionInfo#getFullTopicName()} 为准。
     */
    @Getter
    private final String topic;

    /**
     * 构建分区级消费者管理器。
     * <p>
     * Builder 方法名为 {@code create}，避免与父类 Lombok {@code builder()} 冲突。
     * 内部会调用父类构造并立即以「每分区消费者」配置完成初始化。
     *
     * @param queueKey              队列唯一标识
     * @param topic                 关联的逻辑 Topic 名
     * @param pollInterval          消费轮询间隔（毫秒），写入固定的每分区模式配置
     * @param msgPackProcessor      消息批次业务处理器
     * @param consumerCreator       按配置与分区创建底层消费者的工厂
     * @param queueAdmin            用于删除 Topic 的管理客户端
     * @param consumerExecutor      消费循环线程池
     * @param scheduler             延迟调度线程池（任务锁重试等）
     * @param taskExecutor          管理任务线程池
     * @param uncaughtErrorHandler  消费循环致命异常回调，可为 {@code null}
     */
    @Builder(builderMethodName = "create") // not to conflict with super.builder()
    public PartitionedQueueConsumerManager(QueueKey queueKey, String topic, long pollInterval, MsgPackProcessor<M, QueueConfig> msgPackProcessor,
                                           BiFunction<QueueConfig, TopicPartitionInfo, TbQueueConsumer<M>> consumerCreator, TbQueueAdmin queueAdmin,
                                           ExecutorService consumerExecutor, ScheduledExecutorService scheduler,
                                           ExecutorService taskExecutor, Consumer<Throwable> uncaughtErrorHandler) {
        super(queueKey, QueueConfig.of(true, pollInterval), msgPackProcessor, consumerCreator, consumerExecutor, scheduler, taskExecutor, uncaughtErrorHandler);
        this.topic = topic;
        this.consumerWrapper = (ConsumerPerPartitionWrapper) super.consumerWrapper;
        this.queueAdmin = queueAdmin;
    }

    /**
     * 处理本类特有的增量分区任务（在父类任务循环中、合并配置/全量分区之前即时执行）。
     * <p>
     * 支持的任务类型：
     * <ul>
     *   <li>{@link AddPartitionsTask}：为新分区创建消费者并启动；可带停止回调与起始 offset；</li>
     *   <li>{@link RemovePartitionsTask}：仅停止并移除对应分区消费者，不删 Topic；</li>
     *   <li>{@link DeletePartitionsTask}：先移除消费者，再通过 {@link #queueAdmin} 删除各分区 Topic，
     *       单个 Topic 删除失败只记日志，不影响其它分区。</li>
     * </ul>
     * 未识别的任务类型由父类空实现忽略（本方法不调用 {@code super}）。
     *
     * @param task 从任务队列取出的管理任务
     */
    @Override
    protected void processTask(TbQueueConsumerManagerTask task) {
        if (task instanceof AddPartitionsTask addPartitionsTask) {
            log.info("[{}] Added partitions: {}", queueKey, addPartitionsTask.partitions());
            consumerWrapper.addPartitions(addPartitionsTask.partitions(), addPartitionsTask.onStop(), addPartitionsTask.startOffsetProvider());
        } else if (task instanceof RemovePartitionsTask removePartitionsTask) {
            log.info("[{}] Removed partitions: {}", queueKey, removePartitionsTask.partitions());
            consumerWrapper.removePartitions(removePartitionsTask.partitions());
        } else if (task instanceof DeletePartitionsTask deletePartitionsTask) {
            log.info("[{}] Removing partitions and deleting topics: {}", queueKey, deletePartitionsTask.partitions());
            consumerWrapper.removePartitions(deletePartitionsTask.partitions());
            deletePartitionsTask.partitions().forEach(tpi -> {
                String topic = tpi.getFullTopicName();
                try {
                    queueAdmin.deleteTopic(topic);
                } catch (Throwable t) {
                    log.error("Failed to delete topic {}", topic, t);
                }
            });
        }
    }

    /**
     * 异步为指定分区增加消费者（无停止回调、无自定义起始 offset）。
     *
     * @param partitions 待新增并开始消费的分区集合
     * @see #addPartitions(Set, Consumer, Function)
     */
    public void addPartitions(Set<TopicPartitionInfo> partitions) {
        addPartitions(partitions, null, null);
    }

    /**
     * 异步为指定分区增加消费者，并可附带生命周期与位点控制。
     * <p>
     * 实际创建/订阅/启动在任务线程中执行。{@code onStop} 会在该分区消费循环退出后回调；
     * {@code startOffsetProvider} 用于 Kafka 等支持指定起始位点的实现。
     *
     * @param partitions          待新增分区
     * @param onStop              分区消费者停止后的回调，可为 {@code null}
     * @param startOffsetProvider 按 Topic 全名提供起始 offset，可为 {@code null}
     */
    public void addPartitions(Set<TopicPartitionInfo> partitions, Consumer<TopicPartitionInfo> onStop, Function<String, Long> startOffsetProvider) {
        addTask(new AddPartitionsTask(partitions, onStop, startOffsetProvider));
    }

    /**
     * 异步停止并移除指定分区上的消费者，不删除底层 Topic。
     * <p>
     * 适用于分区所有权迁移、临时缩容等仍需保留 Topic 数据的场景。
     *
     * @param partitions 待移除消费职责的分区集合
     */
    public void removePartitions(Set<TopicPartitionInfo> partitions) {
        addTask(new RemovePartitionsTask(partitions));
    }

    /**
     * 异步移除分区消费者并删除对应 Topic。
     * <p>
     * 适用于动态 Topic 生命周期结束（如租户队列销毁）等需要清理存储资源的场景。
     * Topic 删除失败不会回滚消费者移除，仅记录错误日志。
     *
     * @param partitions 待删除的分区（其 full topic name 将交给 {@link TbQueueAdmin}）
     */
    public void delete(Set<TopicPartitionInfo> partitions) {
        addTask(new DeletePartitionsTask(partitions));
    }

}
