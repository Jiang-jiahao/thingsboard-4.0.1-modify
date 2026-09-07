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
 * 轻量级「单消费者」管理器：把「创建一个 {@link TbQueueConsumer} + 订阅 + 在独立线程里跑 poll 循环 + 停机退订」
 * 这套最小生命周期封装成可复用组件。
 *
 * <h2>在消费者体系中的位置</h2>
 * ThingsBoard 队列消费侧还有更重量级的管理器，例如：
 * <ul>
 *   <li>{@link MainQueueConsumerManager}：带配置变更、任务队列、分区重平衡等编排能力；</li>
 *   <li>{@link PartitionedQueueConsumerManager}：按分区组织多消费者。</li>
 * </ul>
 * 本类刻意不做这些事：
 * <ul>
 *   <li>不管理多个消费者实例（内部永远只有一个 {@link #consumer}）；</li>
 *   <li>不维护异步任务队列去合并/串行化「改配置 / 改分区」；</li>
 *   <li>不感知服务发现或集群分区变更状态机——分区怎么变，完全由调用方自己决定何时再调 {@link #subscribe(Set)}。</li>
 * </ul>
 * 因此更适合：单一 Topic、订阅关系简单、分区集合由外部一次性给定（或极少变更）的消费路径。
 *
 * <h2>推荐使用顺序</h2>
 * <ol>
 *   <li>用 {@link Builder} 构建实例——构造时就会通过 {@code consumerCreator} <b>立即创建</b>底层消费者；</li>
 *   <li>调用 {@link #subscribe()} 或 {@link #subscribe(Set)} 完成订阅；</li>
 *   <li>调用 {@link #launch()}：向 {@code consumerExecutor} 提交一个长期运行的消费循环任务；</li>
 *   <li>停机时调用 {@link #stop()}：置 {@link #stopped} 并 {@code unsubscribe}；循环线程在下一轮条件检查时退出。</li>
 * </ol>
 *
 * <h2>消费循环行为摘要</h2>
 * <ul>
 *   <li>循环条件：{@code !stopped && !consumer.isStopped()}；</li>
 *   <li>{@code poll(pollInterval)} 得到空列表则直接下一轮；</li>
 *   <li>非空则交给 {@link MsgPackProcessor#process}；</li>
 *   <li>处理抛异常且消费者未停：打 warn，再 {@code sleep(pollInterval)} 作为退避，然后继续循环；</li>
 *   <li>循环外层若再抛出未捕获的 {@link Throwable}：只打 error 日志，<b>不会自动重启</b>循环；</li>
 *   <li>本类没有独立的 uncaughtErrorHandler 回调。</li>
 * </ul>
 *
 * <h2>线程与停机注意点</h2>
 * <ul>
 *   <li>{@link #launch()} 只是向线程池 submit，不等待循环真正跑起来；</li>
 *   <li>{@link #stop()} 也不 join 消费线程；若需要严格等待任务结束，需调用方自行配合关闭/等待 {@link #consumerExecutor}；</li>
 *   <li>{@link #stopped} 为 {@code volatile}，保证 stop 与循环线程之间的可见性。</li>
 * </ul>
 *
 * @param <M> 队列消息类型，需实现 {@link TbQueueMsg}
 * @see MainQueueConsumerManager
 * @see PartitionedQueueConsumerManager
 * @see TbQueueConsumerTask
 */
@Slf4j
public class QueueConsumerManager<M extends TbQueueMsg> {

    /**
     * 管理器逻辑名称，主要用于日志前缀（例如队列名、业务模块名），便于多消费者场景下区分来源。
     */
    private final String name;

    /**
     * 消息批次业务处理器。
     * <p>
     * 由调用方在构建时注入；仅在 poll 得到<strong>非空</strong>消息列表后调用。
     * 处理器内部通常负责业务处理，以及按需提交 offset / 确认消费（通过入参里的 {@link TbQueueConsumer}）。
     */
    private final MsgPackProcessor<M> msgPackProcessor;

    /**
     * 单次 poll 的超时/等待间隔（毫秒）。
     * <p>
     * 有两处用途：
     * <ol>
     *   <li>传给 {@link TbQueueConsumer#poll(long)}，控制阻塞拉取的等待时长；</li>
     *   <li>批次处理失败后的退避休眠时间，避免异常风暴打满 CPU / 下游。</li>
     * </ol>
     */
    private final long pollInterval;

    /**
     * 执行消费循环的线程池。
     * <p>
     * {@link #launch()} 会向其提交一个长期运行的 Runnable；本类不负责创建或关闭该线程池，
     * 生命周期由调用方管理。
     */
    private final ExecutorService consumerExecutor;

    /**
     * 可选的消费线程名前缀。
     * <p>
     * 非 {@code null} 时，在循环任务刚启动时调用
     * {@link ThingsBoardThreadFactory#addThreadNamePrefix(String)}，
     * 方便在线程 dump、APM、日志里识别这条消费链路。为 {@code null} 则不改线程名。
     */
    private final String threadPrefix;

    /**
     * 本管理器唯一持有的底层队列消费者。
     * <p>
     * 在构造方法里通过 {@code consumerCreator.get()} <b>只创建一次</b>，之后所有
     * subscribe / poll / unsubscribe 都作用在这一实例上。可通过 getter 暴露给外部做诊断或扩展操作。
     */
    @Getter
    private final TbQueueConsumer<M> consumer;

    /**
     * 软停止标志。
     * <p>
     * {@link #stop()} 将其置为 {@code true} 后，{@link #consumerLoop} 在下一轮 while 条件判断时退出。
     * 使用 {@code volatile} 保证跨线程可见；置位本身不会立刻中断正在进行的 poll/process。
     */
    private volatile boolean stopped;

    /**
     * 通过 Lombok {@link Builder} 构建管理器。
     * <p>
     * <b>重要：</b>底层 {@link TbQueueConsumer} 在构造阶段就会被创建（调用一次 {@code consumerCreator}），
     * 但此时尚未订阅、也尚未启动循环。调用方必须继续调用 {@link #subscribe()} / {@link #launch()}。
     *
     * @param name             日志用逻辑名称，建议与队列或业务模块对应
     * @param msgPackProcessor 非空批次的业务处理回调
     * @param pollInterval     poll 超时及失败退避时间（毫秒）
     * @param consumerCreator  底层消费者工厂；仅在构造时 {@code get()} 一次
     * @param consumerExecutor 承载消费循环的线程池（由外部提供与关闭）
     * @param threadPrefix     消费线程名前缀；传 {@code null} 表示不修改线程名
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
     * 按底层消费者的默认策略订阅。
     * <p>
     * 通常表示订阅配置中的完整 Topic，而不限定具体分区集合。
     * 最终行为完全取决于 {@link TbQueueConsumer#subscribe()} 的实现（Kafka / in-memory 等可能不同）。
     * <p>
     * 建议在 {@link #launch()} 之前调用；若在循环已运行时调用，是否安全取决于底层消费者是否支持运行时重订阅。
     */
    public void subscribe() {
        consumer.subscribe();
    }

    /**
     * 订阅指定的 Topic 分区集合。
     * <p>
     * 可在启动前调用，也可由调用方在运行期按需再次调用以调整订阅范围。
     * <b>本类没有内置分区变更任务队列或互斥锁</b>：若并发地一边 poll、一边改订阅，
     * 线程安全与语义正确性完全依赖底层 {@link TbQueueConsumer#subscribe(Set)} 实现。
     *
     * @param partitions 目标分区集合（Topic + partition 信息）
     */
    public void subscribe(Set<TopicPartitionInfo> partitions) {
        consumer.subscribe(partitions);
    }

    /**
     * 异步启动消费循环。
     * <p>
     * 向 {@link #consumerExecutor} 提交一个任务：
     * <ol>
     *   <li>若配置了 {@link #threadPrefix}，先给当前工作线程加上名称前缀；</li>
     *   <li>进入 {@link #consumerLoop(TbQueueConsumer)}；</li>
     *   <li>若循环内抛出未捕获的 {@link Throwable}，记录 error 后结束任务（不会自动 restart）；</li>
     *   <li>无论正常因 stop 退出还是异常退出，最后都会打一条「Consumer stopped」info 日志。</li>
     * </ol>
     * 本方法立即返回，不保证循环已经开始执行。重复调用 {@code launch()} 会再提交一个循环任务，
     * 调用方应自行保证只 launch 一次，除非明确需要并行多循环（通常不推荐，且共享同一 consumer）。
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
     * 消费主循环：反复 poll，非空批次交给 {@link #msgPackProcessor}。
     * <p>
     * <b>退出条件（满足其一即结束循环）：</b>
     * <ul>
     *   <li>{@link #stopped} 已被 {@link #stop()} 置为 true；</li>
     *   <li>底层 {@code consumer.isStopped()} 返回 true（例如外部已 unsubscribe / 关闭）。</li>
     * </ul>
     * <b>单次迭代：</b>
     * <ol>
     *   <li>{@code consumer.poll(pollInterval)} 拉取一批消息；</li>
     *   <li>空列表 → {@code continue}，不回调业务；</li>
     *   <li>非空 → {@code msgPackProcessor.process(msgs, consumer)}；</li>
     *   <li>process 抛 {@link Exception} 且消费者仍未停止：打 warn，再 sleep {@link #pollInterval} 退避；
     *       sleep 被中断时仅打 trace，不重新设置中断状态；</li>
     *   <li>若异常发生时消费者已经 stopped，则吞掉异常并不再 sleep，直接进入下一次 while 判断并退出。</li>
     * </ol>
     * 本方法不负责 unsubscribe；退订由 {@link #stop()} 完成。
     *
     * @param consumer 已创建的底层消费者（通常调用方已先完成 subscribe）
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
     * 请求停止消费。
     * <p>
     * 执行两步：
     * <ol>
     *   <li>将 {@link #stopped} 置为 {@code true}，使循环在下一轮条件检查时退出；</li>
     *   <li>调用 {@code consumer.unsubscribe()}，促使底层尽快结束阻塞中的 poll（具体取决于实现）。</li>
     * </ol>
     * <b>非阻塞：</b>本方法不等待消费线程真正退出。若正在 {@code process} 长耗时业务，会等该次处理结束后才离开循环。
     * 需要严格等待时，请由调用方结合线程池的 {@code shutdown}/{@code awaitTermination} 等策略处理。
     */
    public void stop() {
        log.debug("[{}] Stopping consumer", name);
        stopped = true;
        consumer.unsubscribe();
    }

    /**
     * 消息批次处理回调接口。
     * <p>
     * 与 {@link MainQueueConsumerManager.MsgPackProcessor} 相比，本接口不传入
     * {@link org.thingsboard.server.common.data.queue.QueueConfig}：
     * poll 间隔等运行参数在管理器构造时已经固化，业务侧如需其它配置应自行闭包捕获。
     *
     * @param <M> 消息类型，与外层管理器一致
     */
    public interface MsgPackProcessor<M extends TbQueueMsg> {

        /**
         * 处理一批从队列拉取的消息。
         * <p>
         * 调用约定：
         * <ul>
         *   <li>{@code msgs} 在进入本方法前已保证非空；</li>
         *   <li>{@code consumer} 即拉取这批消息的同一实例，可用于提交 offset、查询状态等；</li>
         *   <li>抛出 {@link Exception} 会被 {@link QueueConsumerManager#consumerLoop} 捕获并触发退避后重试下一轮 poll
         *       （注意：默认不会自动重新投递「已 poll 但未成功处理」的同一批消息，除非底层 consumer 在失败时不提交 offset）。</li>
         * </ul>
         *
         * @param msgs     本轮 poll 得到的消息列表（非空）
         * @param consumer 拉取这些消息的消费者
         * @throws Exception 处理失败时抛出，触发循环内 warn + sleep 退避
         */
        void process(List<M> msgs, TbQueueConsumer<M> consumer) throws Exception;
    }

}
