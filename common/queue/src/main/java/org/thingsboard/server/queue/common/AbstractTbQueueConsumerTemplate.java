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
 * 抽象的消息队列消费者模板类，实现了 {@link TbQueueConsumer} 接口。
 * 该类封装了通用的消费者逻辑，包括订阅管理、轮询、提交、停止等操作。
 * 子类需要实现具体的与消息中间件交互的方法（如 doPoll、doSubscribe 等）。
 *
 * @param <R> 原始消息记录类型（如 Kafka 的 ConsumerRecord）
 * @param <T> 解码后的消息类型，需实现 {@link TbQueueMsg}
 */
@Slf4j
public abstract class AbstractTbQueueConsumerTemplate<R, T extends TbQueueMsg> implements TbQueueConsumer<T> {

    /** 一毫秒对应的纳秒数，用于睡眠计算 */
    public static final long ONE_MILLISECOND_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    /** 是否已经订阅（即已调用 doSubscribe） */
    private volatile boolean subscribed;

    /** 消费者是否已停止 */
    protected volatile boolean stopped = false;

    /** 当前订阅的分区集合 */
    protected volatile Set<TopicPartitionInfo> partitions;

    /** 可重入锁，用于保护订阅变更和轮询操作的线程安全 */
    protected final ReentrantLock consumerLock = new ReentrantLock(); //NonfairSync

    /**
     * 订阅请求队列，用于暂存外部线程发起的订阅/重订阅操作。
     * 由于 poll 方法在消费者线程中被循环调用，通过队列将订阅请求传递给 poll 线程处理，
     * 避免了直接在外部线程中操作底层消费者（可能引发并发问题）。
     */
    final Queue<Set<TopicPartitionInfo>> subscribeQueue = new ConcurrentLinkedQueue<>();

    /** 消费者监听的原始主题名称（用于单主题订阅） */
    @Getter
    private final String topic;

    /**
     * 构造函数，指定主题名称。
     * @param topic 主题名称
     */
    public AbstractTbQueueConsumerTemplate(String topic) {
        this.topic = topic;
    }

    /**
     * 订阅默认主题（即构造函数传入的 topic）。
     * 将订阅请求（单主题分区信息）放入队列，由 poll 线程实际执行。
     */
    @Override
    public void subscribe() {
        log.debug("enqueue topic subscribe {} ", topic);
        if (stopped) {
            log.error("trying subscribe, but consumer stopped for topic {}", topic);
            return;
        }
        // 创建 TopicPartitionInfo，useInternalPartition 固定为 true，分区未指定
        subscribeQueue.add(Collections.singleton(new TopicPartitionInfo(topic, null, null, true)));
    }

    /**
     * 订阅指定的分区集合。
     * @param partitions 要订阅的分区信息集合
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
     * 轮询获取消息，阻塞指定时间。
     * 处理流程：
     * 1. 如果消费者已停止，直接返回空列表。
     * 2. 如果尚未订阅且无待处理订阅请求，则睡眠剩余时间后返回空。
     * 3. 加锁处理订阅变更：从队列中取出最新的订阅请求，执行 doSubscribe。
     * 4. 调用 doPoll 获取原始记录列表。
     * 5. 若未获取到记录且不支持长轮询，则睡眠剩余时间后返回空；否则立即解码并返回。
     *
     * @param durationInMillis 最大阻塞时间（毫秒）
     * @return 解码后的消息列表，可能为空
     */
    @Override
    public List<T> poll(long durationInMillis) {
        List<R> records;
        long startNanos = System.nanoTime();
        if (stopped) {
            log.error("poll invoked but consumer stopped for topic " + topic, new RuntimeException("stacktrace"));
            return emptyList();
        }
        // 如果未订阅且没有待处理的订阅请求，直接睡眠等待（模拟轮询效果）
        // 这个地方partitions == null条件不能去除，如果去除，在doSubscribe方法异常的时候，会会出现一直睡眠的情况
        if (!subscribed && partitions == null && subscribeQueue.isEmpty()) {
            return sleepAndReturnEmpty(startNanos, durationInMillis);
        }

        // 检查锁状态，仅用于调试，若锁已被持有可能表示竞争或死锁
        if (consumerLock.isLocked()) {
            log.error("poll. consumerLock is locked. will wait with no timeout. it looks like a race conditions or deadlock topic " + topic, new RuntimeException("stacktrace"));
        }

        consumerLock.lock();
        try {
            // 在拉取消息之前，先处理所有待处理的订阅请求
            // （防止在拉取的时候，订阅被改变，出现线程安全问题。由于 Kafka 的 Consumer 对象本身不是线程安全的，如果多个线程同时操作它，会出问题）
            while (!subscribeQueue.isEmpty()) {
                subscribed = false; // 标记需要重新订阅
                partitions = subscribeQueue.poll(); // 取最新的订阅集合
            }
            // 如果尚未订阅或订阅变更，则执行订阅
            if (!subscribed) {
                log.info("Subscribing to {}", partitions);
                doSubscribe(partitions);
                subscribed = true;
            }
            // 如果 partitions 为空（理论上不会，但防御性处理），则直接返回空；否则调用 doPoll
            records = partitions.isEmpty() ? emptyList() : doPoll(durationInMillis);
        } finally {
            consumerLock.unlock();
        }

        // 如果未拉取到任何记录，且底层不支持长轮询（如普通轮询需要主动睡眠），则睡眠剩余时间后返回空
        if (records.isEmpty() && !isLongPollingSupported()) {
            return sleepAndReturnEmpty(startNanos, durationInMillis);
        }

        // 解码原始记录并返回
        return decodeRecords(records);
    }

    /**
     * 将原始记录列表解码为 {@link TbQueueMsg} 列表。
     * @param records 原始记录列表
     * @return 解码后的消息列表
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
     * 睡眠剩余时间并返回空列表。
     * 用于模拟轮询阻塞，在无消息且不支持长轮询时调用。
     * @param startNanos 开始轮询的纳秒时间
     * @param durationInMillis 期望阻塞的毫秒数
     * @return 空列表
     */
    List<T> sleepAndReturnEmpty(final long startNanos, final long durationInMillis) {
        // 将期望阻塞时间转换为纳秒
        long durationNanos = TimeUnit.MILLISECONDS.toNanos(durationInMillis);
        // 计算从轮询开始到当前已经过去的时间（纳秒）
        long spentNanos = System.nanoTime() - startNanos;
        // 剩余需要等待的时间（纳秒）
        long nanosLeft = durationNanos - spentNanos;
        // 判断是否需要睡眠，如果剩余时间大于1毫秒，则睡眠
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
     * 提交已消费的消息偏移量。
     * 加锁保护，避免与 poll 并发。
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
     * 停止消费者（标记停止，但不主动取消订阅）。
     */
    @Override
    public void stop() {
        stopped = true;
    }

    /**
     * 取消订阅并停止消费者。
     * 先标记停止，然后加锁执行底层取消订阅操作。
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
     * 检查消费者是否已停止。
     * @return true 表示已停止
     */
    @Override
    public boolean isStopped() {
        return stopped;
    }

    /**
     * 执行实际的消息轮询。
     * @param durationInMillis 超时时间（毫秒）
     * @return 原始记录列表
     */
    abstract protected List<R> doPoll(long durationInMillis);

    /**
     * 将原始记录解码为应用层消息。
     * @param record 原始记录
     * @return 解码后的消息
     * @throws IOException 解码异常
     */
    abstract protected T decode(R record) throws IOException;

    /**
     * 执行实际订阅操作。
     * @param partitions 要订阅的分区集合
     */
    abstract protected void doSubscribe(Set<TopicPartitionInfo> partitions);

    /**
     * 执行实际提交偏移量操作。
     */
    abstract protected void doCommit();

    /**
     * 执行实际取消订阅操作。
     */
    abstract protected void doUnsubscribe();

    /**
     * 获取当前订阅的所有完整主题名称（包含分区信息）。
     * @return 主题名称列表
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
     * 判断底层消息中间件是否支持长轮询（即 poll 方法本身会阻塞直到有消息或超时）。
     * 若返回 false，则上层会在无消息时主动 sleep 剩余时间。
     * @return 默认 false，子类可重写
     */
    protected boolean isLongPollingSupported() {
        return false;
    }

}
