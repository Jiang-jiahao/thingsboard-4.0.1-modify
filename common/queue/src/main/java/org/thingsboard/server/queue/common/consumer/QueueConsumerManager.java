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
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueMsg;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * 轻量级单消费者管理器：封装「一个 {@link TbQueueConsumer} + 一条 poll 循环」的最小生命周期。
 * <p>
 * 与 {@link MainQueueConsumerManager} / {@link PartitionedQueueConsumerManager} 相比，本类
 * <strong>不</strong>处理：
 * <ul>
 *   <li>多消费者 / 每分区消费者编排；</li>
 *   <li>异步任务队列与配置、分区的合并更新；</li>
 *   <li>服务发现驱动的分区变更状态机。</li>
 * </ul>
 * 适合订阅关系简单、由调用方显式控制 subscribe / launch / stop 的场景（例如单一 Topic、
 * 或分区集合由外部一次性给定且变更极少的消费路径）。
 * <p>
 * <b>使用顺序建议：</b>
 * <ol>
 *   <li>Builder 构建实例（构造时即通过 {@code consumerCreator} 创建底层消费者）；</li>
 *   <li>{@link #subscribe()} 或 {@link #subscribe(Set)} 完成订阅；</li>
 *   <li>{@link #launch()} 在 {@code consumerExecutor} 中启动消费循环；</li>
 *   <li>停机时调用 {@link #stop()}：置停止标志并 {@code unsubscribe}。</li>
 * </ol>
 * <p>
 * 消费循环逻辑与主管理器类似：空批次跳过；处理异常时按 {@code pollInterval} 退避；
 * 循环因未捕获异常退出时仅打错误日志（无独立的 uncaughtErrorHandler 回调）。
 *
 * @param <M> 队列消息类型，需实现 {@link TbQueueMsg}
 * @see MainQueueConsumerManager
 * @see PartitionedQueueConsumerManager
 */
@Slf4j
public class QueueConsumerManager<M extends TbQueueMsg> {

    /**
     * 管理器逻辑名称，主要用于日志标识（如队列名、业务模块名）。
     */
    private final String name;

    /**
     * 消息批次处理器，由业务方注入，在 poll 到非空列表后执行。
     */
    private final MsgPackProcessor<M> msgPackProcessor;

    /**
     * 每次 poll 的阻塞/等待间隔（毫秒），同时作为处理失败后的退避休眠时间。
     */
    private final long pollInterval;

    /**
     * 执行消费循环的线程池；{@link #launch()} 会向其提交一个长期运行的任务。
     */
    private final ExecutorService consumerExecutor;

    /**
     * 可选的消费线程名前缀。
     * <p>
     * 非空时在循环启动前通过 {@link ThingsBoardThreadFactory#addThreadNamePrefix} 设置，
     * 便于在线程 dump / 监控中区分不同消费链路。
     */
    private final String threadPrefix;

    /**
     * 本管理器唯一持有的底层队列消费者实例。
     * <p>
     * 在构造阶段由 {@code consumerCreator.get()} 创建，之后通过 subscribe / poll /
     * unsubscribe 管理其状态。
     */
    @Getter
    private final TbQueueConsumer<M> consumer;

    /**
     * 停止标志。置为 {@code true} 后，消费循环在下一轮条件检查时退出。
     */
    private volatile boolean stopped;

    /**
     * 使用 Builder 构建轻量消费者管理器。
     * <p>
     * 注意：底层 {@link TbQueueConsumer} 在构造时立即创建，但尚未订阅、也未启动循环；
     * 需由调用方继续调用 {@link #subscribe()} / {@link #launch()}。
     *
     * @param name             日志用名称
     * @param msgPackProcessor 消息批次业务处理器
     * @param pollInterval     poll 间隔及失败退避时间（毫秒）
     * @param consumerCreator  底层消费者供应器（只调用一次）
     * @param consumerExecutor 运行消费循环的线程池
     * @param threadPrefix     线程名前缀，可为 {@code null} 表示不修改
     */
    @Builder
    public QueueConsumerManager(String name, MsgPackProcessor<M> msgPackProcessor,
                                long pollInterval, Supplier<TbQueueConsumer<M>> consumerCreator,
                                ExecutorService consumerExecutor, String threadPrefix) {
        this.name = name;
        this.pollInterval = pollInterval;
        this.msgPackProcessor = msgPackProcessor;
        this.consumerExecutor = consumerExecutor;
        this.threadPrefix = threadPrefix;
        this.consumer = consumerCreator.get();
    }

    /**
     * 按消费者默认策略订阅（通常为配置中的完整 Topic，不限定分区集合）。
     * <p>
     * 具体行为取决于底层 {@link TbQueueConsumer#subscribe()} 实现。
     */
    public void subscribe() {
        consumer.subscribe();
    }

    /**
     * 订阅指定的 Topic 分区集合。
     * <p>
     * 可在启动前调用，也可在运行中由调用方按需再次调用以调整订阅（本类不内置
     * 分区变更任务队列，是否支持运行时重订阅取决于底层消费者实现）。
     *
     * @param partitions 目标分区集合
     */
    public void subscribe(Set<TopicPartitionInfo> partitions) {
        consumer.subscribe(partitions);
    }

    /**
     * 在 {@link #consumerExecutor} 中异步启动消费循环。
     * <p>
     * 若配置了 {@link #threadPrefix}，会先为当前线程添加名称前缀，再进入
     * {@link #consumerLoop}。循环因异常或停止退出后打印停止日志；致命异常仅记录，
     * 不会自动重启循环。
     */
    public void launch() {
        log.info("[{}] Launching consumer", name);
        consumerExecutor.submit(() -> {
            if (threadPrefix != null) {
                ThingsBoardThreadFactory.addThreadNamePrefix(threadPrefix);
            }
            try {
                consumerLoop(consumer);
            } catch (Throwable e) {
                log.error("Failure in consumer loop", e);
            }
            log.info("[{}] Consumer stopped", name);
        });
    }

    /**
     * 消费主循环：反复 poll，非空则交给 {@link #msgPackProcessor}。
     * <p>
     * 退出条件：{@link #stopped} 为 true，或底层 {@code consumer.isStopped()}。
     * 单次处理异常且消费者未停止时，按 {@link #pollInterval} sleep 后继续；
     * 已被中断则仅打 trace 日志。本方法不负责 unsubscribe，退订由 {@link #stop()} 完成。
     *
     * @param consumer 已创建（通常已订阅）的底层消费者
     */
    private void consumerLoop(TbQueueConsumer<M> consumer) {
        while (!stopped && !consumer.isStopped()) {
            try {
                List<M> msgs = consumer.poll(pollInterval);
                if (msgs.isEmpty()) {
                    continue;
                }
                msgPackProcessor.process(msgs, consumer);
            } catch (Exception e) {
                if (!consumer.isStopped()) {
                    log.warn("Failed to process messages from queue", e);
                    try {
                        Thread.sleep(pollInterval);
                    } catch (InterruptedException interruptedException) {
                        log.trace("Failed to wait until the server has capacity to handle new requests", interruptedException);
                    }
                }
            }
        }
    }

    /**
     * 停止消费：设置停止标志并取消订阅。
     * <p>
     * 消费循环线程将在下一次条件判断时退出。本方法不等待线程池任务真正结束；
     * 若需要严格等待，需由调用方结合 {@link #consumerExecutor} 的关闭策略处理。
     */
    public void stop() {
        log.debug("[{}] Stopping consumer", name);
        stopped = true;
        consumer.unsubscribe();
    }

    /**
     * 消息批次处理回调。
     * <p>
     * 相对 {@link MainQueueConsumerManager.MsgPackProcessor}，本接口不传入
     * {@link org.thingsboard.server.common.data.queue.QueueConfig}，配置相关参数
     * 由管理器在构造时固化（如 poll 间隔）。
     *
     * @param <M> 消息类型
     */
    public interface MsgPackProcessor<M extends TbQueueMsg> {
        /**
         * 处理一批从队列拉取的消息。
         *
         * @param msgs     消息列表（调用前已保证非空）
         * @param consumer 拉取这些消息的消费者，可用于提交 offset 等
         * @throws Exception 处理失败时抛出，触发循环内退避重试
         */
        void process(List<M> msgs, TbQueueConsumer<M> consumer) throws Exception;
    }

}
