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
package org.thingsboard.server.queue.common;

import jakarta.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueMsg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.emptyList;

/**
 * 面向「外部消息中间件」的消费者模板基类（典型实现：{@code TbKafkaConsumerTemplate}）。
 * <p>
 * 本类实现 {@link TbQueueConsumer} 的通用骨架，把「线程安全的订阅变更」「poll 生命周期」
 * 「原始记录解码」「无消息时的阻塞语义」抽到上层；子类只需实现与具体中间件交互的
 * {@link #doSubscribe} / {@link #doPoll} / {@link #decode} / {@link #doCommit} / {@link #doUnsubscribe}。
 * <p>
 * <b>设计动机（为何不直接在业务线程调 Kafka Consumer）：</b>
 * <ul>
 *   <li>Kafka 等客户端的 Consumer 对象通常<strong>非线程安全</strong>，subscribe/poll/commit
 *       必须在同一消费线程上串行执行。</li>
 *   <li>上层（如 {@code MainQueueConsumerManager}）往往在管理线程里调用 {@link #subscribe}，
 *       而真正拉消息在独立的 consumer loop 线程里调用 {@link #poll}。</li>
 *   <li>因此本类用 {@link #subscribeQueue} 暂存订阅请求，只在 {@link #poll} 持锁时真正执行
 *       {@link #doSubscribe}，保证底层消费者始终被单线程访问。</li>
 * </ul>
 * <p>
 * <b>典型调用时序：</b>
 * <pre>
 *   管理线程: subscribe(partitions)  → 请求入队
 *   消费线程: poll()                 → 出队并 doSubscribe → doPoll → decode → 返回消息
 *   消费线程: commit()               → doCommit（与 poll 互斥）
 *   管理/停止: unsubscribe() / stop() → 标记停止并/或 doUnsubscribe
 * </pre>
 * <p>
 * <b>与内存实现的关系：</b>
 * 进程内队列（{@code InMemoryTbQueueConsumer}）逻辑简单、无中间件线程约束，直接实现
 * {@link TbQueueConsumer}，不继承本模板。本模板主要为 Kafka 等 broker 客户端服务。
 * <p>
 * <b>类型参数：</b>
 *
 * @param <R> 中间件原始记录类型（如 Kafka {@code ConsumerRecord<String, byte[]>}）
 * @param <T> 解码后的应用层消息，须实现 {@link TbQueueMsg}
 */
@Slf4j
public abstract class AbstractTbQueueConsumerTemplate<R, T extends TbQueueMsg> implements TbQueueConsumer<T> {

    /** 1 毫秒对应的纳秒数；{@link #sleepAndReturnEmpty} 里剩余等待时间小于该值则不再 sleep */
    public static final long ONE_MILLISECOND_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    /**
     * 底层消息队列是否已对当前 {@link #partitions} 完成过 {@link #doSubscribe}。
     * <p>
     * 置为 {@code false} 的时机：从 {@link #subscribeQueue} 取出新订阅集合时。
     * 下次 {@link #poll} 会重新调用 {@link #doSubscribe}。
     */
    private volatile boolean subscribed;

    /**
     * 消费者停止标记。
     * <p>
     * {@link #stop()} / {@link #unsubscribe()} 会置为 {@code true}；
     * 之后 {@link #subscribe} 会被拒绝，{@link #poll} 直接返回空列表。
     */
    protected volatile boolean stopped = false;

    /**
     * 当前生效（或即将生效）的订阅分区集合。
     * <p>
     * 由 {@link #poll} 从 {@link #subscribeQueue} 取出后赋值；
     * {@code null} 表示尚未收到过任何订阅请求。
     */
    protected volatile Set<TopicPartitionInfo> partitions;

    /**
     * 保护「订阅变更 + poll + commit + unsubscribe」对底层消费者的互斥访问。
     * <p>
     * 使用非公平锁（ReentrantLock 默认 NonfairSync）：吞吐优先，不保证等待线程 FIFO。
     * 若进入 poll/commit 时发现锁已被占用，会打 error 日志（疑似并发误用或死锁），但仍会阻塞等待。
     */
    protected final ReentrantLock consumerLock = new ReentrantLock(); // NonfairSync

    /**
     * 订阅请求队列（跨线程传递）。
     * <p>
     * <ul>
     *   <li>生产端：任意线程调用 {@link #subscribe()} / {@link #subscribe(Set)} 时，只把分区集合入队，
     *       绝不在此直接调用底层 {@code doSubscribe}。</li>
     *   <li>消费端：{@link #poll} 持锁后，一次性排空队列；若多次入队，以<strong>最后一次</strong>
     *       poll 出的集合为准（中间结果被覆盖丢弃）。</li>
     * </ul>
     * 这样既满足 Kafka Consumer 单线程约束，又允许管理线程随时推送分区变更
     * （例如收到 {@code PartitionChangeEvent} 后更新订阅）。
     */
    final Queue<Set<TopicPartitionInfo>> subscribeQueue = new ConcurrentLinkedQueue<>();

    /**
     * 该消费者关联的逻辑 topic 名（构造时传入）。
     * <p>
     * 无参 {@link #subscribe()} 会基于此 topic 构造一个分区号为空的 {@link TopicPartitionInfo}
     *（四参构造：{@code myPartition=true}，{@code useInternalPartition} 固定为 {@code false}）。
     * 按分区精确订阅时走 {@link #subscribe(Set)}，集合内的 topic 可能已带集群前缀 / 外置分区后缀。
     */
    @Getter
    private final String topic;

    /**
     * @param topic 默认逻辑 topic；无参 {@link #subscribe()} 时使用
     */
    public AbstractTbQueueConsumerTemplate(String topic) {
        this.topic = topic;
    }

    /**
     * 订阅构造时指定的默认 topic（不指定具体分区号）。
     * <p>
     * 仅将请求放入 {@link #subscribeQueue}，真正的中间件订阅延迟到下一次 {@link #poll}。
     * 若已 {@link #stop}，则拒绝入队并打 error。
     * <p>
     * 使用四参数构造 {@code new TopicPartitionInfo(topic, null, null, true)}：
     * <ul>
     *   <li>末参 {@code true} 是 {@code myPartition}，不是 {@code useInternalPartition}；</li>
     *   <li>四参构造内部把 {@code useInternalPartition} 写死为 {@code false}（当前主路径惯例）；</li>
     *   <li>{@code partition == null} → 不拼 {@code topic.N} 后缀，表示按整 topic 订阅，
     *       具体分区分配交给中间件或 {@link #doSubscribe}。</li>
     * </ul>
     */
    @Override
    public void subscribe() {
        log.debug("enqueue topic subscribe {} ", topic);
        if (stopped) {
            log.error("trying subscribe, but consumer stopped for topic {}", topic);
            return;
        }
        // 四参构造：(topic, tenantId, partition, myPartition)
        // → useInternalPartition=false，myPartition=true；partition 为空表示整 topic 订阅
        subscribeQueue.add(Collections.singleton(new TopicPartitionInfo(topic, null, null, true)));
    }

    /**
     * 订阅指定的 {@link TopicPartitionInfo} 集合（ThingsBoard 常用：按分区精确 assign）。
     * <p>
     * 同样只入队，由 {@link #poll} 线程执行 {@link #doSubscribe}。
     * 可被多次调用；队列中积压多条时，poll 侧只保留最后一次取出的集合。
     *
     * @param partitions 目标分区集合；空集合通常表示退订所有分区（具体行为由 {@link #doSubscribe} 解释）
     */
    @Override
    public void subscribe(Set<TopicPartitionInfo> partitions) {
        log.debug("enqueue topics subscribe {} ", partitions);
        if (stopped) {
            log.error("trying subscribe, but consumer stopped for topic {}", topic);
            return;
        }
        subscribeQueue.add(partitions);
    }

    /**
     * 拉取一批消息（最多阻塞约 {@code durationInMillis} 毫秒）。
     * <p>
     * <b>完整流程：</b>
     * <ol>
     *   <li>若已停止 → 打 error（带 stacktrace）并返回空列表。</li>
     *   <li>若尚未有任何订阅（{@code !subscribed && partitions==null && 队列空}）→
     *       主动 sleep 剩余时间后返回空，避免空转占满 CPU。
     *       <br>注意：{@code partitions == null} 条件不能去掉——若 {@link #doSubscribe} 失败导致
     *       {@code subscribed} 仍为 false 但 {@code partitions} 已非 null，去掉该条件会一直走 sleep，
     *       再也无法重试订阅。</li>
     *   <li>加 {@link #consumerLock}：排空 {@link #subscribeQueue}，必要时 {@link #doSubscribe}，
     *       再 {@link #doPoll} 取原始记录。</li>
     *   <li>若无记录且 {@link #isLongPollingSupported()} 为 false（如当前 Kafka 实现默认）→
     *       再 sleep 补足本次 poll 期望的阻塞时长，保证上层循环的节奏稳定。</li>
     *   <li>{@link #decodeRecords} 将 {@code List<R>} 转为 {@code List<T>} 返回。</li>
     * </ol>
     *
     * @param durationInMillis 期望的最长阻塞时间（毫秒），传给 {@link #doPoll}，并用于无消息时的补睡
     * @return 解码后的消息列表；无消息或未订阅时可能为空，不会返回 null
     */
    @Override
    public List<T> poll(long durationInMillis) {
        List<R> records;
        long startNanos = System.nanoTime();
        if (stopped) {
            log.error("poll invoked but consumer stopped for topic " + topic, new RuntimeException("stacktrace"));
            return emptyList();
        }
        // 从未订阅过：模拟阻塞等待，避免 busy-loop
        // partitions==null 必须保留，见类注释与上方流程说明
        if (!subscribed && partitions == null && subscribeQueue.isEmpty()) {
            return sleepAndReturnEmpty(startNanos, durationInMillis);
        }

        // 调试用：锁已被占用时记录，便于排查管理线程与消费线程的竞态
        if (consumerLock.isLocked()) {
            log.error("poll. consumerLock is locked. will wait with no timeout. it looks like a race conditions or deadlock topic " + topic, new RuntimeException("stacktrace"));
        }

        consumerLock.lock();
        try {
            // 先处理积压的订阅变更，再 poll——保证底层 Consumer 不被多线程同时操作
            while (!subscribeQueue.isEmpty()) {
                subscribed = false; // 强制下次（或本次循环末尾）重新 doSubscribe
                partitions = subscribeQueue.poll(); // 多次入队时，循环结束留下的是最后一次
            }
            if (!subscribed) {
                log.info("Subscribing to {}", partitions);
                doSubscribe(partitions);
                subscribed = true;
            }
            // 防御：分区集合为空则不调用中间件 poll
            records = partitions.isEmpty() ? emptyList() : doPoll(durationInMillis);
        } finally {
            consumerLock.unlock();
        }

        // 短轮询中间件：doPoll 可能立刻返回空，需由模板补齐阻塞时间
        if (records.isEmpty() && !isLongPollingSupported()) {
            return sleepAndReturnEmpty(startNanos, durationInMillis);
        }

        return decodeRecords(records);
    }

    /**
     * 将中间件原始记录批量解码为应用消息。
     * <p>
     * 单条 {@code null} 记录会被跳过；任一非空记录解码失败会打 error 并抛出
     * {@link RuntimeException}，避免脏数据静默进入业务处理。
     *
     * @param records 原始记录列表，不应为 null
     * @return 解码成功的消息列表（长度 ≤ records.size()）
     */
    @Nonnull
    List<T> decodeRecords(@Nonnull List<R> records) {
        List<T> result = new ArrayList<>(records.size());
        records.forEach(record -> {
            try {
                if (record != null) {
                    result.add(decode(record));
                }
            } catch (Exception e) {
                log.error("Failed to decode record {}", record, e);
                throw new RuntimeException("Failed to decode record " + record, e);
            }
        });
        return result;
    }

    /**
     * 在「本次 poll 已耗时」的基础上，睡眠剩余时间并返回空列表。
     * <p>
     * 用于：
     * <ul>
     *   <li>尚未订阅时的等待；</li>
     *   <li>短轮询且本轮无消息时，补足 {@code durationInMillis}，避免上层紧循环空转。</li>
     * </ul>
     * 剩余时间不足 1ms 则不再 sleep。若已 {@link #stop}，中断异常只打 trace 级处理（不刷 error）。
     *
     * @param startNanos       本轮 {@link #poll} 开始时的 {@link System#nanoTime()}
     * @param durationInMillis 本轮期望总阻塞毫秒数
     * @return 恒为空列表
     */
    List<T> sleepAndReturnEmpty(final long startNanos, final long durationInMillis) {
        long durationNanos = TimeUnit.MILLISECONDS.toNanos(durationInMillis);
        long spentNanos = System.nanoTime() - startNanos;
        long nanosLeft = durationNanos - spentNanos;
        if (nanosLeft >= ONE_MILLISECOND_IN_NANOS) {
            try {
                long sleepMs = TimeUnit.NANOSECONDS.toMillis(nanosLeft);
                log.trace("Going to sleep after poll: topic {} for {}ms", topic, sleepMs);
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                if (!stopped) {
                    log.error("Failed to wait", e);
                }
            }
        }
        return emptyList();
    }

    /**
     * 提交已成功处理消息的消费位移（具体语义由中间件决定，Kafka 即为 offset commit）。
     * <p>
     * 与 {@link #poll} 共用 {@link #consumerLock}，避免 commit 与 poll/subscribe 并发打到同一 Consumer。
     * 若进入时锁已被占用且尚未停止，会打 error（疑似竞态），然后阻塞等待锁。
     */
    @Override
    public void commit() {
        if (consumerLock.isLocked()) {
            if (stopped) {
                return;
            }
            log.error("commit. consumerLock is locked. will wait with no timeout. it looks like a race conditions or deadlock topic " + topic, new RuntimeException("stacktrace"));
        }
        consumerLock.lock();
        try {
            doCommit();
        } finally {
            consumerLock.unlock();
        }
    }

    /**
     * 软停止：仅将 {@link #stopped} 置为 {@code true}。
     * <p>
     * 不会调用 {@link #doUnsubscribe()}，也不会清空 {@link #subscribeQueue}。
     * 适用于「先通知循环退出，稍后再 {@link #unsubscribe} 做底层清理」的场景。
     * 停止后新的 {@link #subscribe} 会被拒绝，{@link #poll} 返回空。
     */
    @Override
    public void stop() {
        stopped = true;
    }

    /**
     * 取消订阅并停止消费者。
     * <p>
     * 先置 {@link #stopped}，再持锁：若当前 {@link #subscribed} 为 true，则调用
     * {@link #doUnsubscribe()} 释放中间件侧订阅/连接资源。
     */
    @Override
    public void unsubscribe() {
        log.info("Unsubscribing and stopping consumer for {}", partitions);
        stopped = true;
        consumerLock.lock();
        try {
            if (subscribed) {
                doUnsubscribe();
            }
        } finally {
            consumerLock.unlock();
        }
    }

    /**
     * @return {@code true} 表示已调用过 {@link #stop()} 或 {@link #unsubscribe()}
     */
    @Override
    public boolean isStopped() {
        return stopped;
    }

    /**
     * 子类实现：从中间件拉取原始记录。
     * <p>
     * 调用时已持有 {@link #consumerLock}，且 {@link #doSubscribe} 已对应当前 {@link #partitions} 执行过。
     *
     * @param durationInMillis 超时/最长阻塞毫秒数（语义由中间件 API 决定）
     * @return 原始记录列表；无数据时返回空列表而非 null
     */
    abstract protected List<R> doPoll(long durationInMillis);

    /**
     * 子类实现：将单条原始记录解码为 {@link TbQueueMsg}。
     *
     * @param record 原始记录
     * @return 应用层消息
     * @throws IOException 解码失败时抛出（会被 {@link #decodeRecords} 包装为 RuntimeException）
     */
    abstract protected T decode(R record) throws IOException;

    /**
     * 子类实现：对底层消费者执行真正的订阅/分区分配。
     * <p>
     * 仅在 {@link #poll} 持锁线程中调用。Kafka 实现中常见为 {@code assign} 指定分区，
     * 或 {@code subscribe} 整 topic。
     *
     * @param partitions 本次要生效的分区集合（可能为空）
     */
    abstract protected void doSubscribe(Set<TopicPartitionInfo> partitions);

    /**
     * 子类实现：提交消费进度。
     * <p>
     * 仅在 {@link #commit} 持锁线程中调用。
     */
    abstract protected void doCommit();

    /**
     * 子类实现：取消订阅并关闭/清理底层消费者资源。
     * <p>
     * 仅在 {@link #unsubscribe} 持锁且此前已成功 {@link #doSubscribe} 时调用。
     */
    abstract protected void doUnsubscribe();

    /**
     * 返回当前订阅分区对应的完整 topic 名列表（含前缀等，见 {@link TopicPartitionInfo#getFullTopicName()}）。
     *
     * @return 若尚未订阅过（{@link #partitions} 为 null）则返回空列表
     */
    @Override
    public List<String> getFullTopicNames() {
        if (partitions == null) {
            return Collections.emptyList();
        }
        return partitions.stream()
                .map(TopicPartitionInfo::getFullTopicName)
                .toList();
    }

    /**
     * 底层 poll 是否自身会阻塞到有数据或超时（长轮询）。
     * <p>
     * <ul>
     *   <li>{@code true}：无消息时不必再由本模板 {@link #sleepAndReturnEmpty}；</li>
     *   <li>{@code false}（默认）：短轮询，无消息时模板补睡，保证调用方感知到的阻塞接近
     *       {@code durationInMillis}。</li>
     * </ul>
     * 当前 Kafka 实现通常保持默认 {@code false}；若某中间件 poll 已具备长阻塞语义，子类可重写为 true。
     *
     * @return 默认 {@code false}
     */
    protected boolean isLongPollingSupported() {
        return false;
    }

}
