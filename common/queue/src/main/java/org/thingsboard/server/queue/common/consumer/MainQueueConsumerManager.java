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

@Slf4j
public class MainQueueConsumerManager<M extends TbQueueMsg, C extends QueueConfig> {

    /**
     * 队列的唯一标识符（如租户ID或服务实例ID）
     */
    @Getter
    protected final QueueKey queueKey;

    /**
     * 当前队列配置（包含消费模式、轮询间隔等参数）
     */
    @Getter
    protected C config;

    /**
     * 消息处理器（实际业务逻辑）
     */
    protected final MsgPackProcessor<M, C> msgPackProcessor;

    /**
     * 消费者创建工厂函数
     * 参数1: 队列配置
     * 参数2: 分区信息（可为null）
     * 返回: 初始化的队列消费者
     */
    protected final BiFunction<C, TopicPartitionInfo, TbQueueConsumer<M>> consumerCreator;

    /**
     * 消费者线程池（执行实际消费循环）
     */
    @Getter
    protected final ExecutorService consumerExecutor;


    @Getter
    protected final ScheduledExecutorService scheduler;

    /**
     * 任务处理线程池（执行配置/分区更新）
     */
    @Getter
    protected final ExecutorService taskExecutor;

    /**
     * 未捕获异常处理器
     */
    protected final Consumer<Throwable> uncaughtErrorHandler;

    // 待处理任务队列（线程安全）
    private final java.util.Queue<TbQueueConsumerManagerTask> tasks = new ConcurrentLinkedQueue<>();

    // 任务处理锁（防止并发修改状态）
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 当前分配的分区集合（volatile保证可见性）
     */
    @Getter
    private volatile Set<TopicPartitionInfo> partitions;

    /**
     * 消费者包装器（根据配置选择单/多消费者模式）
     */
    protected volatile ConsumerWrapper<M> consumerWrapper;

    /**
     * 服务停止标志
     */
    protected volatile boolean stopped;

    /**
     * 构造器（Builder模式）
     *
     * @param queueKey 队列唯一标识
     * @param config 初始队列配置
     * @param msgPackProcessor 消息处理器
     * @param consumerCreator 消费者创建函数
     * @param consumerExecutor 消费者线程池
     * @param scheduler 调度线程池
     * @param taskExecutor 任务处理线程池
     * @param uncaughtErrorHandler 异常处理器
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
     * 初始化消费者管理器
     *
     * @param config 队列配置
     */
    public void init(C config) {
        this.config = config;
        this.consumerWrapper = createConsumerWrapper(config);
        log.debug("[{}] Initialized consumer for queue: {}", queueKey, config);
    }

    /**
     * 根据配置创建消费者包装器
     *
     * @param config 队列配置
     * @return 消费者包装器实例
     */
    protected ConsumerWrapper<M> createConsumerWrapper(C config) {
        // 根据配置选择消费模式
        if (config.isConsumerPerPartition()) {
            // 创建每个分区独立消费者模式（也就是一个分区一个消费者）
            return new ConsumerPerPartitionWrapper();
        } else {
            // 创建单消费者模式
            return new SingleConsumerWrapper();
        }
    }

    /**
     * 更新队列配置（异步任务）
     *
     * @param config 新队列配置
     */
    public void update(C config) {
        addTask(new UpdateConfigTask(config));
    }

    /**
     * 更新分区分配（异步任务）
     *
     * @param partitions 新分区集合
     */
    public void update(Set<TopicPartitionInfo> partitions) {
        addTask(new UpdatePartitionsTask(partitions));
    }

    /**
     * 添加任务到队列并触发处理
     *
     * @param todo 待处理任务
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
     * 尝试处理任务队列（非阻塞方式）
     */
    private void tryProcessTasks() {
        taskExecutor.submit(() -> {
            if (lock.tryLock()) {
                try {
                    C newConfig = null;
                    Set<TopicPartitionInfo> newPartitions = null;
                    while (!stopped) {
                        TbQueueConsumerManagerTask task = tasks.poll();
                        // 没有任务，则退出循环执行
                        if (task == null) {
                            break;
                        }
                        log.trace("[{}] Processing task: {}", queueKey, task);
                        // 任务分类处理
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
                    // 队列运行时，并且空闲的时候，会进行配置变更
                    if (newConfig != null) {
                        doUpdate(newConfig);
                    }
                    // 队列运行时，并且空闲的时候，会进行分区变更
                    if (newPartitions != null) {
                        doUpdate(newPartitions);
                    }
                } catch (Exception e) {
                    log.error("[{}] Failed to process tasks", queueKey, e);
                } finally {
                    lock.unlock();
                }
            } else {
                // 获取锁失败，1秒后重试。这里切换由scheduler线程池执行
                log.trace("[{}] Failed to acquire lock", queueKey);
                scheduler.schedule(this::tryProcessTasks, 1, TimeUnit.SECONDS);
            }
        });
    }

    /**
     * 处理自定义任务（子类可扩展）
     *
     * @param task 待处理任务
     */
    protected void processTask(TbQueueConsumerManagerTask task) {
    }

    /**
     * 执行配置更新
     *
     * @param newConfig 新配置
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
                doUpdate(partitions); // even if partitions number was changed, there can be no partition change event
            }
        } else {
            log.trace("[{}] Silently applied new config, because consumer-per-partition not changed", queueKey);
            // do nothing, because partitions change (if they changed) will be handled on PartitionChangeEvent,
            // and changes to other config values will be picked up by consumer on the fly,
            // and queue topic and name are immutable
        }
    }

    /**
     * 执行分区更新
     *
     * @param partitions 新分区集合
     */
    private void doUpdate(Set<TopicPartitionInfo> partitions) {
        this.partitions = partitions;
        consumerWrapper.updatePartitions(partitions);
    }

    /**
     * 启动消费者线程
     *
     * @param consumerTask 消费者任务
     */
    private void launchConsumer(TbQueueConsumerTask<M> consumerTask) {
        log.info("[{}] Launching consumer", consumerTask.getKey());
        Future<?> consumerLoop = consumerExecutor.submit(() -> {
            ThingsBoardThreadFactory.updateCurrentThreadName(consumerTask.getKey().toString());
            consumerLoop(consumerTask.getConsumer());
            // 当执行到这里的时候，标识消费者标识位已经被改为停止状态
            log.info("[{}] Consumer stopped", consumerTask.getKey());

            try {
                Runnable callback = consumerTask.getCallback();
                // 若果consumer 停止的回调函数不为空，则执行
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
     * 消费者主循环
     * 里面进行消息的消费，消费者拉取数据，进行消息处理
     *
     * @param consumer 队列消费者实例
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
     * 处理消息批次（模板方法）
     *
     * @param msgs 消息列表
     * @param consumer 消费者实例
     * @param config 队列配置
     * @throws Exception 处理异常
     */
    protected void processMsgs(List<M> msgs, TbQueueConsumer<M> consumer, C config) throws Exception {
        log.trace("Processing {} messages", msgs.size());
        msgPackProcessor.process(msgs, consumer, config);
        log.trace("Processed {} messages", msgs.size());
    }

    public void stop() {
        log.debug("[{}] Stopping consumers", queueKey);
        consumerWrapper.getConsumers().forEach(TbQueueConsumerTask::initiateStop);
        stopped = true;
    }

    public void awaitStop() {
        awaitStop(30);
    }

    /**
     * 等待manager停止，停止manager里面所有的消费者
     * @param timeoutSec
     */
    private void awaitStop(int timeoutSec) {
        log.debug("[{}] Waiting for consumers to stop", queueKey);
        consumerWrapper.getConsumers().forEach(consumerTask -> consumerTask.awaitCompletion(timeoutSec));
        log.debug("[{}] Unsubscribed and stopped consumers", queueKey);
    }

    public interface MsgPackProcessor<M extends TbQueueMsg, C extends QueueConfig> {
        void process(List<M> msgs, TbQueueConsumer<M> consumer, C config) throws Exception;
    }

    /**
     * 消费者包装器接口（统一处理分区变更）
     * 分区和消费者的一个映射器
     *
     * @param <M> 消息类型
     */
    public interface ConsumerWrapper<M extends TbQueueMsg> {

        /**
         * 更新分区分配
         *
         * @param partitions 新分区集合
         */
        void updatePartitions(Set<TopicPartitionInfo> partitions);

        /**
         * 获取所有消费者任务
         *
         * @return 消费者任务集合
         */
        Collection<TbQueueConsumerTask<M>> getConsumers();

    }

    /**
     * 分区级消费者包装器（每个分区独立消费者）
     */
    class ConsumerPerPartitionWrapper implements ConsumerWrapper<M> {
        // 分区 -> 消费者 映射
        private final Map<TopicPartitionInfo, TbQueueConsumerTask<M>> consumers = new HashMap<>();

        @Override
        public void updatePartitions(Set<TopicPartitionInfo> partitions) {
            Set<TopicPartitionInfo> addedPartitions = new HashSet<>(partitions);
            // 计算出需要新增的分区
            addedPartitions.removeAll(consumers.keySet());
            // 计算出需要移除的分区
            Set<TopicPartitionInfo> removedPartitions = new HashSet<>(consumers.keySet());
            removedPartitions.removeAll(partitions);

            log.info("[{}] Added partitions: {}, removed partitions: {}", queueKey, addedPartitions, removedPartitions);
            // 移除分区
            removePartitions(removedPartitions);
            // 新增分区
            addPartitions(addedPartitions, null, null);
        }

        /**
         * 移除分区消费者
         *
         * @param removedPartitions 待移除分区集合
         */
        protected void removePartitions(Set<TopicPartitionInfo> removedPartitions) {
            // 停止分区对应的消费者，设置停止标识
            removedPartitions.forEach((tpi) -> Optional.ofNullable(consumers.get(tpi)).ifPresent(TbQueueConsumerTask::initiateStop));
            // 移除分区和消费者的映射关系，然后等待一定时间停止
            removedPartitions.forEach((tpi) -> Optional.ofNullable(consumers.remove(tpi)).ifPresent(TbQueueConsumerTask::awaitCompletion));
        }

        /**
         * 添加分区消费者
         * 利用消费者创建器创建消费者实例并订阅主题
         *
         * @param partitions 新增分区集合
         * @param onStop 停止回调
         * @param startOffsetProvider 起始偏移量提供器
         */
        protected void addPartitions(Set<TopicPartitionInfo> partitions, Consumer<TopicPartitionInfo> onStop, Function<String, Long> startOffsetProvider) {
            partitions.forEach(tpi -> {

                Integer partitionId = tpi.getPartition().orElse(-1);
                String key = queueKey + "-" + partitionId;
                // 定义消费者停止的时候执行的回调函数
                Runnable callback = onStop != null ? () -> onStop.accept(tpi) : null;
                // 创建队列消费者任务（相当于消费者的包装器）
                TbQueueConsumerTask<M> consumer = new TbQueueConsumerTask<>(key, () -> {
                    // lambda表达式用来创建消费者实例
                    TbQueueConsumer<M> queueConsumer = consumerCreator.apply(config, tpi);
                    // 如果是kafka的消费者。则设置起始偏移量
                    if (startOffsetProvider != null && queueConsumer instanceof TbKafkaConsumerTemplate<M> kafkaConsumer) {
                        kafkaConsumer.setStartOffsetProvider(startOffsetProvider);
                    }
                    return queueConsumer;
                }, callback);
                // 添加消费者和分区的对应关系
                consumers.put(tpi, consumer);
                // 消费者订阅分区
                consumer.subscribe(Set.of(tpi));
                // 启动消费者
                launchConsumer(consumer);
            });
        }

        @Override
        public Collection<TbQueueConsumerTask<M>> getConsumers() {
            return consumers.values();
        }
    }

    /**
     * 单消费者包装器（单个消费者处理所有分区）
     */
    class SingleConsumerWrapper implements ConsumerWrapper<M> {
        private TbQueueConsumerTask<M> consumer;

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
                consumer = new TbQueueConsumerTask<>(queueKey, () -> consumerCreator.apply(config, null), null); // no partitionId passed
            }
            consumer.subscribe(partitions);
            if (!consumer.isRunning()) {
                launchConsumer(consumer);
            }
        }

        @Override
        public Collection<TbQueueConsumerTask<M>> getConsumers() {
            if (consumer == null) {
                return Collections.emptyList();
            }
            return List.of(consumer);
        }
    }
}
