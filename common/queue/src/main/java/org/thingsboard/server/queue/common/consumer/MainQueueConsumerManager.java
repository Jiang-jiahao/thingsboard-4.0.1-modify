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
import org.thingsboard.server.common.data.queue.QueueConfig;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueMsg;
import org.thingsboard.server.queue.common.consumer.TbQueueConsumerManagerTask.UpdateConfigTask;
import org.thingsboard.server.queue.common.consumer.TbQueueConsumerManagerTask.UpdatePartitionsTask;
import org.thingsboard.server.queue.discovery.QueueKey;
import org.thingsboard.server.queue.kafka.TbKafkaConsumerTemplate;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 队列消费者主管理器：ThingsBoard 队列消费体系的核心编排类。
 * <p>
 * 负责把「队列配置 / 分区分配 / 消息消费循环」三件事串在一起，对外屏蔽底层
 * Kafka/InMemory/RabbitMQ 等具体实现差异。典型使用场景是规则引擎、Core 服务等
 * 需要按 QueueKey 管理一组 Topic 分区消费的模块。
 * <p>
 * <b>核心职责：</b>
 * <ul>
 *   <li>根据 {@link QueueConfig#isConsumerPerPartition()} 选择消费模式：
 *       每分区一个消费者（{@link ConsumerPerPartitionWrapper}）或单消费者订阅全部区
 *       （{@link SingleConsumerWrapper}）。</li>
 *   <li>通过异步任务队列处理配置更新、分区变更，避免与消费线程并发修改状态。</li>
 *   <li>在独立线程池中运行 poll → process 消费循环，异常时退避重试，致命错误回调
 *       {@code uncaughtErrorHandler}。</li>
 * </ul>
 * <p>
 * <b>任务处理模型：</b>
 * {@link #update(QueueConfig)} / {@link #update(Set)} 不会同步改状态，而是把任务放入
 * {@link #tasks}，由 {@link #tryProcessTasks()} 在 {@code taskExecutor} 上串行执行。
 * 同一时刻只允许一个线程持有 {@link #lock}；拿不到锁则由 {@code scheduler} 延迟 1 秒重试。
 * 一轮任务处理中，配置变更与分区变更各自只保留「最后一次」生效结果，减少抖动。
 * <p>
 * <b>与子类关系：</b>
 * {@link PartitionedQueueConsumerManager} 继承本类，固定为每分区消费者模式，并扩展
 * 增删分区、删除 Topic 等细粒度任务（走 {@link #processTask} 钩子，不等待配置/分区合并）。
 *
 * @param <M> 队列消息类型，需实现 {@link TbQueueMsg}
 * @param <C> 队列配置类型，需继承 {@link QueueConfig}
 * @see PartitionedQueueConsumerManager
 * @see QueueConsumerManager
 * @see TbQueueConsumerManagerTask
 */
@Slf4j
public class MainQueueConsumerManager<M extends TbQueueMsg, C extends QueueConfig> {

    /**
     * 本管理器对应的队列唯一键。
     * <p>
     * 通常由服务类型、租户、队列名等组成，用于日志关联、线程命名以及区分不同消费实例。
     */
    @Getter
    protected final QueueKey queueKey;

    /**
     * 当前生效的队列配置。
     * <p>
     * 包含 poll 间隔、是否每分区独立消费者等。配置更新经任务队列异步应用；
     * 若 {@code consumerPerPartition} 开关变化，会先停掉旧消费者再按新模式重建。
     */
    @Getter
    protected C config;

    /**
     * 消息批次处理器。
     * <p>
     * 消费循环 poll 到非空消息列表后，统一交给该处理器执行业务逻辑（如规则引擎处理、
     * 持久化、转发等）。由调用方在构建管理器时注入。
     */
    protected final MsgPackProcessor<M, C> msgPackProcessor;

    /**
     * 底层队列消费者工厂。
     * <p>
     * 参数：当前配置、目标分区信息（单消费者模式下分区参数可为 {@code null}）。
     * 返回：已可订阅的 {@link TbQueueConsumer} 实例。
     * 具体创建 Kafka / In-Memory 等实现由上层决定。
     */
    protected final BiFunction<C, TopicPartitionInfo, TbQueueConsumer<M>> consumerCreator;

    /**
     * 执行消费循环（poll + process）的线程池。
     * <p>
     * 每个 {@link TbQueueConsumerTask} 启动时会向该线程池提交一个长期运行的循环任务。
     */
    @Getter
    protected final ExecutorService consumerExecutor;

    /**
     * 调度线程池。
     * <p>
     * 当前主要用于任务锁竞争失败时的延迟重试（{@link #tryProcessTasks} 拿不到锁时
     * 延迟 1 秒再次触发）。
     */
    @Getter
    protected final ScheduledExecutorService scheduler;

    /**
     * 任务处理线程池。
     * <p>
     * 专门跑配置更新、分区更新等管理类任务，与消费线程池隔离，避免管理操作阻塞 poll。
     */
    @Getter
    protected final ExecutorService taskExecutor;

    /**
     * 消费循环中未捕获的致命异常处理器。
     * <p>
     * 当 {@link #consumerLoop} 外层捕获到 {@link Throwable} 时回调，便于上层做告警或重启。
     * 可为 {@code null}，表示不额外处理。
     */
    protected final Consumer<Throwable> uncaughtErrorHandler;

    /**
     * 待处理的管理任务队列（线程安全）。
     * <p>
     * 存放配置更新、分区更新以及子类自定义任务。由 {@link #addTask} 入队，
     * {@link #tryProcessTasks} 出队并串行处理。
     */
    private final java.util.Queue<TbQueueConsumerManagerTask> tasks = new ConcurrentLinkedQueue<>();

    /**
     * 任务处理互斥锁。
     * <p>
     * 保证同一时刻只有一个任务处理流程在修改消费者状态；使用 {@code tryLock} 非阻塞获取，
     * 失败则延迟重试，避免阻塞调用方线程。
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 当前节点被分配到的 Topic 分区集合。
     * <p>
     * 由服务发现 / 分区变更事件驱动更新；{@code volatile} 保证多线程可见性。
     * 实际订阅动作委托给 {@link #consumerWrapper}。
     */
    @Getter
    private volatile Set<TopicPartitionInfo> partitions;

    /**
     * 消费者包装器，封装「分区 ↔ 消费者」的映射与生命周期。
     * <p>
     * 在 {@link #init} 时按配置创建为 {@link ConsumerPerPartitionWrapper} 或
     * {@link SingleConsumerWrapper}；配置中消费模式切换时会整体重建。
     */
    protected volatile ConsumerWrapper<M> consumerWrapper;

    /**
     * 管理器停止标志。
     * <p>
     * 置为 {@code true} 后：不再接受新任务；消费循环在下一轮检查时退出。
     */
    protected volatile boolean stopped;

    /**
     * 使用 Builder 模式构建管理器。
     * <p>
     * 若构造时 {@code config} 非空，会立即调用 {@link #init} 创建消费者包装器；
     * 否则需稍后主动 {@link #init} 或通过 {@link #update(QueueConfig)} 完成初始化。
     *
     * @param queueKey              队列唯一标识
     * @param config                初始队列配置，可为 {@code null}（延迟初始化）
     * @param msgPackProcessor      消息批次业务处理器
     * @param consumerCreator       底层消费者创建工厂
     * @param consumerExecutor      消费循环线程池
     * @param scheduler             延迟调度线程池（任务锁重试等）
     * @param taskExecutor          管理任务执行线程池
     * @param uncaughtErrorHandler  消费循环致命异常回调，可为 {@code null}
     */
    @Builder
    public MainQueueConsumerManager(QueueKey queueKey, C config,
                                    MsgPackProcessor<M, C> msgPackProcessor,
                                    BiFunction<C, TopicPartitionInfo, TbQueueConsumer<M>> consumerCreator,
                                    ExecutorService consumerExecutor,
                                    ScheduledExecutorService scheduler,
                                    ExecutorService taskExecutor,
                                    Consumer<Throwable> uncaughtErrorHandler) {
        this.queueKey = queueKey;
        this.config = config;
        this.msgPackProcessor = msgPackProcessor;
        this.consumerCreator = consumerCreator;
        this.consumerExecutor = consumerExecutor;
        this.scheduler = scheduler;
        this.taskExecutor = taskExecutor;
        this.uncaughtErrorHandler = uncaughtErrorHandler;
        if (config != null) {
            init(config);
        }
    }

    /**
     * 使用给定配置初始化（或重建）消费者包装器。
     * <p>
     * 会覆盖 {@link #config}，并按 {@code consumerPerPartition} 创建对应的
     * {@link ConsumerWrapper}。注意：本方法本身不自动订阅分区，分区仍需通过
     * {@link #update(Set)} / {@link #doUpdate(Set)} 下发。
     *
     * @param config 要生效的队列配置，不可为 {@code null}
     */
    public void init(C config) {
        this.config = config;
        this.consumerWrapper = createConsumerWrapper(config);
        log.debug("[{}] Initialized consumer for queue: {}", queueKey, config);
    }

    /**
     * 按配置选择并创建消费者包装器实现。
     * <p>
     * {@code consumerPerPartition == true} → 每分区独立消费者；
     * 否则 → 单个消费者同时订阅所有分配到的分区。
     * 子类可覆盖以强制某种模式（如 {@link PartitionedQueueConsumerManager}）。
     *
     * @param config 队列配置
     * @return 与配置匹配的 {@link ConsumerWrapper} 实例
     */
    protected ConsumerWrapper<M> createConsumerWrapper(C config) {
        if (config.isConsumerPerPartition()) {
            return new ConsumerPerPartitionWrapper();
        } else {
            return new SingleConsumerWrapper();
        }
    }

    /**
     * 异步更新队列配置。
     * <p>
     * 将 {@link UpdateConfigTask} 加入任务队列，由 {@link #tryProcessTasks} 在适当时机
     * 调用 {@link #doUpdate(QueueConfig)}。若管理器已停止，任务会被直接丢弃。
     *
     * @param config 新的队列配置
     */
    public void update(C config) {
        addTask(new UpdateConfigTask(config));
    }

    /**
     * 异步更新本节点负责的分区集合。
     * <p>
     * 将 {@link UpdatePartitionsTask} 入队，最终由包装器执行增删消费者 / 重新订阅。
     * 同一轮任务合并时只取最后一次分区快照。
     *
     * @param partitions 最新分区集合（空集合表示不再负责任何分区）
     */
    public void update(Set<TopicPartitionInfo> partitions) {
        addTask(new UpdatePartitionsTask(partitions));
    }

    /**
     * 将管理任务加入队列并尝试触发处理。
     * <p>
     * 已停止时直接返回。入队后立即调用 {@link #tryProcessTasks()}，
     * 是否立刻执行取决于能否拿到 {@link #lock}。
     *
     * @param todo 待处理任务（配置更新、分区更新或子类扩展任务）
     */
    protected void addTask(TbQueueConsumerManagerTask todo) {
        if (stopped) {
            return;
        }
        tasks.add(todo);
        log.trace("[{}] Added task: {}", queueKey, todo);
        tryProcessTasks();
    }

    /**
     * 尝试在任务线程池中串行处理队列中的任务。
     * <p>
     * 处理流程：
     * <ol>
     *   <li>向 {@code taskExecutor} 提交处理逻辑；</li>
     *   <li>{@code tryLock} 成功则持续 {@code poll} 任务直至队列为空或已停止；</li>
     *   <li>{@link UpdateConfigTask} / {@link UpdatePartitionsTask} 在本轮内合并为
     *       「最后一次」结果，循环结束后统一 {@link #doUpdate}；</li>
     *   <li>其它任务类型立即交给 {@link #processTask}（供子类扩展，更及时）；</li>
     *   <li>拿不到锁则由 {@code scheduler} 延迟 1 秒再次调用本方法。</li>
     * </ol>
     * 异常会被捕获并打错误日志，不会向外抛出，以免打挂任务线程。
     */
    private void tryProcessTasks() {
        taskExecutor.submit(() -> {
            if (lock.tryLock()) {
                try {
                    C newConfig = null;
                    Set<TopicPartitionInfo> newPartitions = null;
                    while (!stopped) {
                        TbQueueConsumerManagerTask task = tasks.poll();
                        if (task == null) {
                            break;
                        }
                        log.trace("[{}] Processing task: {}", queueKey, task);
                        if (task instanceof UpdatePartitionsTask updatePartitionsTask) {
                            newPartitions = updatePartitionsTask.partitions();
                        } else if (task instanceof UpdateConfigTask updateConfigTask) {
                            newConfig = (C) updateConfigTask.config();
                        } else {
                            processTask(task);
                        }
                    }
                    if (stopped) {
                        return;
                    }
                    if (newConfig != null) {
                        doUpdate(newConfig);
                    }
                    if (newPartitions != null) {
                        doUpdate(newPartitions);
                    }
                } catch (Exception e) {
                    log.error("[{}] Failed to process tasks", queueKey, e);
                } finally {
                    lock.unlock();
                }
            } else {
                log.trace("[{}] Failed to acquire lock", queueKey);
                scheduler.schedule(this::tryProcessTasks, 1, TimeUnit.SECONDS);
            }
        });
    }

    /**
     * 处理非「配置更新 / 分区全量更新」类的自定义任务。
     * <p>
     * 默认空实现。子类（如 {@link PartitionedQueueConsumerManager}）可覆盖此方法，
     * 在任务出队时立即处理增删分区、删 Topic 等操作——这些任务不会进入本轮末尾的合并逻辑，
     * 因此比配置/分区全量更新更及时。
     *
     * @param task 待处理的自定义任务
     */
    protected void processTask(TbQueueConsumerManagerTask task) {
    }

    /**
     * 同步应用新的队列配置（仅在持有任务锁的任务处理线程中调用）。
     * <p>
     * 行为分支：
     * <ul>
     *   <li>旧配置为 {@code null}：视为首次初始化，调用 {@link #init}；</li>
     *   <li>{@code consumerPerPartition} 发生变化：先停止并等待所有旧消费者结束，
     *       再按新模式 {@link #init}；若已有分区分配，再触发一次分区更新以重建订阅；</li>
     *   <li>其它配置项变化（如 poll 间隔）：静默生效，由消费循环下次读取；
     *       Topic/队列名视为不可变，分区变化仍依赖独立的分区变更事件。</li>
     * </ul>
     *
     * @param newConfig 待应用的新配置
     */
    private void doUpdate(C newConfig) {
        log.info("[{}] Processing queue update: {}", queueKey, newConfig);
        var oldConfig = this.config;
        this.config = newConfig;
        if (log.isTraceEnabled()) {
            log.trace("[{}] Old queue configuration: {}", queueKey, oldConfig);
            log.trace("[{}] New queue configuration: {}", queueKey, newConfig);
        }

        if (oldConfig == null) {
            init(config);
        } else if (newConfig.isConsumerPerPartition() != oldConfig.isConsumerPerPartition()) {
            consumerWrapper.getConsumers().forEach(TbQueueConsumerTask::initiateStop);
            consumerWrapper.getConsumers().forEach(TbQueueConsumerTask::awaitCompletion);

            init(config);
            if (partitions != null) {
                // 模式切换后即使未收到新的分区事件，也要用现有分区重建消费者
                doUpdate(partitions);
            }
        } else {
            log.trace("[{}] Silently applied new config, because consumer-per-partition not changed", queueKey);
            // 其它配置由消费循环运行时读取；分区变更由 PartitionChangeEvent 驱动
        }
    }

    /**
     * 同步应用新的分区分配（仅在任务处理线程中调用）。
     * <p>
     * 更新 {@link #partitions} 内存快照，并委托 {@link #consumerWrapper} 增删消费者
     * 或调整订阅集合。
     *
     * @param partitions 新的分区集合
     */
    private void doUpdate(Set<TopicPartitionInfo> partitions) {
        this.partitions = partitions;
        consumerWrapper.updatePartitions(partitions);
    }

    /**
     * 在 {@link #consumerExecutor} 中启动指定消费者的 poll 循环。
     * <p>
     * 线程名会被更新为消费者任务的 key，便于排查。循环正常结束后（停止标志生效），
     * 若任务配置了回调（如分区移除后的清理逻辑），会在此执行。
     *
     * @param consumerTask 封装了底层消费者、Future 与停止回调的任务对象
     */
    private void launchConsumer(TbQueueConsumerTask<M> consumerTask) {
        log.info("[{}] Launching consumer", consumerTask.getKey());
        Future<?> consumerLoop = consumerExecutor.submit(() -> {
            ThingsBoardThreadFactory.updateCurrentThreadName(consumerTask.getKey().toString());
            consumerLoop(consumerTask.getConsumer());
            // 执行到此处说明消费者已进入停止状态，循环已退出
            log.info("[{}] Consumer stopped", consumerTask.getKey());

            try {
                Runnable callback = consumerTask.getCallback();
                if (callback != null) {
                    callback.run();
                }
            } catch (Throwable t) {
                log.error("Failed to execute finish callback", t);
            }
        });
        consumerTask.setTask(consumerLoop);
    }

    /**
     * 单个消费者的主循环：持续 poll 消息并交由业务处理器处理。
     * <p>
     * 退出条件：管理器 {@link #stopped} 或底层 {@code consumer.isStopped()}。
     * 普通处理异常会打警告并按 poll 间隔 sleep 后重试；若消费者已停止则不再重试。
     * 循环因停止退出时会 {@code unsubscribe}；发生致命 {@link Throwable} 时同样退订，
     * 并回调 {@link #uncaughtErrorHandler}（若已配置）。
     *
     * @param consumer 已订阅的底层队列消费者
     */
    private void consumerLoop(TbQueueConsumer<M> consumer) {
        try {
            while (!stopped && !consumer.isStopped()) {
                try {
                    List<M> msgs = consumer.poll(config.getPollInterval());
                    if (msgs.isEmpty()) {
                        continue;
                    }
                    processMsgs(msgs, consumer, config);
                } catch (Exception e) {
                    if (!consumer.isStopped()) {
                        log.warn("Failed to process messages from queue", e);
                        try {
                            Thread.sleep(config.getPollInterval());
                        } catch (InterruptedException e2) {
                            log.trace("Failed to wait until the server has capacity to handle new requests", e2);
                        }
                    }
                }
            }
            if (consumer.isStopped()) {
                consumer.unsubscribe();
            }
        } catch (Throwable t) {
            log.error("Failure in consumer loop", t);
            if (uncaughtErrorHandler != null) {
                uncaughtErrorHandler.accept(t);
            }
            consumer.unsubscribe();
        }
    }

    /**
     * 处理一次 poll 得到的消息批次。
     * <p>
     * 默认实现直接委托 {@link #msgPackProcessor}。子类可覆盖以增加监控、限流、
     * 批量拆分等横切逻辑。
     *
     * @param msgs     本批次消息，调用前已保证非空
     * @param consumer 拉取到这些消息的消费者（可用于提交 offset 等）
     * @param config   当前队列配置
     * @throws Exception 业务处理失败时抛出，由 {@link #consumerLoop} 捕获并退避重试
     */
    protected void processMsgs(List<M> msgs, TbQueueConsumer<M> consumer, C config) throws Exception {
        log.trace("Processing {} messages", msgs.size());
        msgPackProcessor.process(msgs, consumer, config);
        log.trace("Processed {} messages", msgs.size());
    }

    /**
     * 请求停止本管理器下的所有消费者。
     * <p>
     * 向各消费者发出停止信号，并置 {@link #stopped}，之后不再接受新管理任务。
     * 本方法不等待消费线程真正结束，完整等待请调用 {@link #awaitStop()}。
     */
    public void stop() {
        log.debug("[{}] Stopping consumers", queueKey);
        consumerWrapper.getConsumers().forEach(TbQueueConsumerTask::initiateStop);
        stopped = true;
    }

    /**
     * 等待所有消费者在默认超时（30 秒）内完成停止。
     * <p>
     * 通常在 {@link #stop()} 之后调用，用于优雅停机。
     */
    public void awaitStop() {
        awaitStop(30);
    }

    /**
     * 等待本管理器下所有消费者任务结束。
     * <p>
     * 对每个 {@link TbQueueConsumerTask} 调用 {@code awaitCompletion}，最长等待
     * {@code timeoutSec} 秒。超时后仍可能有线程未结束，调用方需结合日志判断。
     *
     * @param timeoutSec 单个消费者等待完成的超时时间（秒）
     */
    private void awaitStop(int timeoutSec) {
        log.debug("[{}] Waiting for consumers to stop", queueKey);
        consumerWrapper.getConsumers().forEach(consumerTask -> consumerTask.awaitCompletion(timeoutSec));
        log.debug("[{}] Unsubscribed and stopped consumers", queueKey);
    }

    /**
     * 消息批次处理回调接口。
     * <p>
     * 由业务方实现，在消费循环中对一批消息执行处理，并负责在适当时机提交 offset
     * （具体语义取决于底层 {@link TbQueueConsumer} 实现）。
     *
     * @param <M> 消息类型
     * @param <C> 配置类型
     */
    public interface MsgPackProcessor<M extends TbQueueMsg, C extends QueueConfig> {
        /**
         * 处理一批从队列中拉取的消息。
         *
         * @param msgs     消息列表
         * @param consumer 对应的消费者实例
         * @param config   当前配置
         * @throws Exception 处理失败时抛出，触发消费循环的退避重试
         */
        void process(List<M> msgs, TbQueueConsumer<M> consumer, C config) throws Exception;
    }

    /**
     * 消费者包装器：统一封装「分区分配变化 → 消费者生命周期」的策略差异。
     * <p>
     * 两种实现对应两种消费模型：
     * <ul>
     *   <li>{@link ConsumerPerPartitionWrapper}：分区与消费者一一对应；</li>
     *   <li>{@link SingleConsumerWrapper}：一个消费者订阅全部当前分区。</li>
     * </ul>
     *
     * @param <M> 消息类型
     */
    public interface ConsumerWrapper<M extends TbQueueMsg> {

        /**
         * 根据最新分区集合调整内部消费者：创建、停止或重新订阅。
         *
         * @param partitions 当前应负责的全部分区
         */
        void updatePartitions(Set<TopicPartitionInfo> partitions);

        /**
         * 返回当前由本包装器管理的全部消费者任务，供停止、等待完成等统一操作使用。
         *
         * @return 消费者任务集合；无消费者时可为空集合
         */
        Collection<TbQueueConsumerTask<M>> getConsumers();

    }

    /**
     * 每分区独立消费者模式的包装器。
     * <p>
     * 内部维护 {@code TopicPartitionInfo → TbQueueConsumerTask} 映射。
     * 分区增加时创建并启动新消费者（只订阅该分区）；分区移除时先发停止信号再等待
     * 线程结束并移除映射。该模式隔离性更好，单分区故障或积压不易拖垮其它分区，
     * 但占用更多线程与连接资源。
     * <p>
     * {@link #addPartitions} / {@link #removePartitions} 同时供子类
     * {@link PartitionedQueueConsumerManager} 做增量分区操作时直接调用。
     */
    class ConsumerPerPartitionWrapper implements ConsumerWrapper<M> {
        /** 分区到消费者任务的映射表 */
        private final Map<TopicPartitionInfo, TbQueueConsumerTask<M>> consumers = new HashMap<>();

        /**
         * 全量对齐分区：计算相对当前映射的新增集与移除集，先删后增。
         *
         * @param partitions 最新应持有的全部分区
         */
        @Override
        public void updatePartitions(Set<TopicPartitionInfo> partitions) {
            Set<TopicPartitionInfo> addedPartitions = new HashSet<>(partitions);
            addedPartitions.removeAll(consumers.keySet());
            Set<TopicPartitionInfo> removedPartitions = new HashSet<>(consumers.keySet());
            removedPartitions.removeAll(partitions);

            log.info("[{}] Added partitions: {}, removed partitions: {}", queueKey, addedPartitions, removedPartitions);
            removePartitions(removedPartitions);
            addPartitions(addedPartitions, null, null);
        }

        /**
         * 停止并移除指定分区上的消费者。
         * <p>
         * 先对每个目标消费者 {@code initiateStop}，再从映射中移除并 {@code awaitCompletion}，
         * 确保消费线程退出后再认为分区已释放。
         *
         * @param removedPartitions 需要移除的分区集合
         */
        protected void removePartitions(Set<TopicPartitionInfo> removedPartitions) {
            removedPartitions.forEach((tpi) -> Optional.ofNullable(consumers.get(tpi)).ifPresent(TbQueueConsumerTask::initiateStop));
            removedPartitions.forEach((tpi) -> Optional.ofNullable(consumers.remove(tpi)).ifPresent(TbQueueConsumerTask::awaitCompletion));
        }

        /**
         * 为新增分区创建消费者、订阅并启动消费循环。
         * <p>
         * 每个分区对应一个 {@link TbQueueConsumerTask}，key 形如 {@code queueKey-partitionId}。
         * 若提供了 {@code startOffsetProvider} 且底层为 Kafka 消费者，会设置起始 offset，
         * 常用于需要从指定位点重放的场景。
         *
         * @param partitions          待新增的分区
         * @param onStop              该分区消费者停止后的回调，可为 {@code null}
         * @param startOffsetProvider 按 Topic 名提供起始 offset，可为 {@code null}
         */
        protected void addPartitions(Set<TopicPartitionInfo> partitions, Consumer<TopicPartitionInfo> onStop, Function<String, Long> startOffsetProvider) {
            partitions.forEach(tpi -> {

                Integer partitionId = tpi.getPartition().orElse(-1);
                String key = queueKey + "-" + partitionId;
                Runnable callback = onStop != null ? () -> onStop.accept(tpi) : null;
                TbQueueConsumerTask<M> consumer = new TbQueueConsumerTask<>(key, () -> {
                    TbQueueConsumer<M> queueConsumer = consumerCreator.apply(config, tpi);
                    if (startOffsetProvider != null && queueConsumer instanceof TbKafkaConsumerTemplate<M> kafkaConsumer) {
                        kafkaConsumer.setStartOffsetProvider(startOffsetProvider);
                    }
                    return queueConsumer;
                }, callback);
                consumers.put(tpi, consumer);
                consumer.subscribe(Set.of(tpi));
                launchConsumer(consumer);
            });
        }

        /**
         * @return 当前所有分区消费者任务
         */
        @Override
        public Collection<TbQueueConsumerTask<M>> getConsumers() {
            return consumers.values();
        }
    }

    /**
     * 单消费者模式的包装器：一个消费者实例订阅本节点分配到的全部区。
     * <p>
     * 分区集合变为空时停止并清空消费者；首次分配到分区时创建消费者并启动；
     * 之后分区变化只调用 {@code subscribe} 更新订阅，若尚未运行则启动循环。
     * 创建消费者时向 {@code consumerCreator} 传入的分区参数为 {@code null}，
     * 由订阅接口一次性绑定多个分区。资源占用更少，但分区间处理互相影响。
     */
    class SingleConsumerWrapper implements ConsumerWrapper<M> {
        /** 唯一的消费者任务；无分区分配时为 {@code null} */
        private TbQueueConsumerTask<M> consumer;

        /**
         * 根据最新分区集合调整唯一消费者的生命周期与订阅。
         *
         * @param partitions 最新分区；空集合表示停止并释放消费者
         */
        @Override
        public void updatePartitions(Set<TopicPartitionInfo> partitions) {
            log.info("[{}] New partitions: {}", queueKey, partitions);
            if (partitions.isEmpty()) {
                if (consumer != null && consumer.isRunning()) {
                    consumer.initiateStop();
                    consumer.awaitCompletion();
                }
                consumer = null;
                return;
            }

            if (consumer == null) {
                // 单消费者模式不按分区创建，故 partition 传 null
                consumer = new TbQueueConsumerTask<>(queueKey, () -> consumerCreator.apply(config, null), null);
            }
            consumer.subscribe(partitions);
            if (!consumer.isRunning()) {
                launchConsumer(consumer);
            }
        }

        /**
         * @return 唯一消费者组成的列表；尚未创建时返回空列表
         */
        @Override
        public Collection<TbQueueConsumerTask<M>> getConsumers() {
            if (consumer == null) {
                return Collections.emptyList();
            }
            return List.of(consumer);
        }
    }
}
