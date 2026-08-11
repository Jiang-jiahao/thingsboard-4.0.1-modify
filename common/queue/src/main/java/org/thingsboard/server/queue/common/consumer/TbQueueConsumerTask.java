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

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.msg.queue.TopicPartitionInfo;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueMsg;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 单个队列消费者的生命周期封装。
 * <p>
 * 本类不负责业务消息处理，也不负责分区分配策略；它把底层
 * {@link TbQueueConsumer}、在线程池中运行的 poll 循环 {@link Future}、
 * 以及循环退出后的收尾回调绑成一个可统一操作的对象，供
 * {@link MainQueueConsumerManager} 及其 {@code ConsumerWrapper} 使用。
 * <p>
 * 典型协作流程：
 * <ol>
 *   <li>构造时只保存 {@link #key}、惰性创建用的 {@link #consumerSupplier} 与可选 {@link #callback}，
 *       此时尚未建立队列连接；</li>
 *   <li>{@link #subscribe(Set)} / {@link #getConsumer()} 首次触发时通过 supplier 创建真正的
 *       {@link TbQueueConsumer}；</li>
 *   <li>管理器调用 {@code launchConsumer} 将 poll 循环提交到线程池，并把返回的
 *       {@link Future} 通过 {@link #setTask(Future)} 写回本对象；</li>
 *   <li>需要停机或摘除分区时，先 {@link #initiateStop()} 通知底层消费者停止，
 *       再 {@link #awaitCompletion()} 等待线程真正退出；</li>
 *   <li>循环正常结束后，由启动方在同一线程中执行 {@link #callback}（若有）。</li>
 * </ol>
 * 与 {@link TbQueueConsumerManagerTask} 的区别：后者是「配置/分区等管理指令」入队任务；
 * 本类是「正在（或即将）运行的消费线程」句柄。
 *
 * @param <M> 队列消息类型
 * @see MainQueueConsumerManager
 * @see TbQueueConsumer
 */
@Slf4j
public class TbQueueConsumerTask<M extends TbQueueMsg> {

    /**
     * 本消费者任务的业务标识，用于日志与线程命名。
     * <p>
     * 常见取值：
     * <ul>
     *   <li>单消费者模式：队列键 {@code queueKey}；</li>
     *   <li>每分区一消费者模式：{@code queueKey-partitionId}。</li>
     * </ul>
     * {@link MainQueueConsumerManager} 启动循环时会把当前线程名更新为 {@code key.toString()}，
     * 便于排查卡顿或泄漏的消费线程。
     */
    @Getter
    private final Object key;

    /**
     * 底层队列消费者实例。
     * <p>
     * 构造后为 {@code null}，在首次 {@link #getConsumer()} 时由 {@link #consumerSupplier}
     * 创建并赋值。使用 {@code volatile} 保证多线程下「已创建」可见；真正创建过程由
     * {@code synchronized} 双重检查保护，避免重复建连。
     */
    private volatile TbQueueConsumer<M> consumer;

    /**
     * 惰性创建底层消费者的工厂。
     * <p>
     * 创建成功后会被置为 {@code null}，避免重复持有闭包/配置引用。
     * 若在 supplier 已被清空后再次需要创建（正常路径不应发生），
     * {@link #getConsumer()} 会因 null 检查失败而抛出异常。
     */
    private volatile Supplier<TbQueueConsumer<M>> consumerSupplier;

    /**
     * 消费循环正常退出后的收尾回调，可为 {@code null}。
     * <p>
     * 不由本类直接调用：由 {@link MainQueueConsumerManager} 在 poll 循环返回后、
     * 同一消费线程中执行。典型用途是「每分区消费者」在分区被移除、线程结束后
     * 通知上层做清理。回调抛出的异常由启动方捕获并记错误日志，不应影响已完成的停机。
     */
    @Getter
    private final Runnable callback;

    /**
     * 消费循环在线程池中的执行句柄。
     * <p>
     * {@code null} 表示当前认为未在运行（尚未启动，或 {@link #awaitCompletion(int)}
     * 已结束后被清空）。非 {@code null} 时 {@link #isRunning()} 为 {@code true}。
     * 由管理器在 {@code submit} 之后通过 {@link #setTask(Future)} 注入；
     * 本类用它做停机等待（{@link Future#get()}），并不负责取消任务
     * （停止语义走底层 {@link TbQueueConsumer#stop()}）。
     */
    @Setter
    private Future<?> task;

    /**
     * 创建消费者任务包装，此时不建立队列连接、不启动线程。
     *
     * @param key              任务标识，见 {@link #key}
     * @param consumerSupplier 首次访问消费者时调用的创建逻辑，不可为 {@code null}
     *                         （在首次 {@link #getConsumer()} 时校验）
     * @param callback         循环退出后的可选回调，没有则传 {@code null}
     */
    public TbQueueConsumerTask(Object key, Supplier<TbQueueConsumer<M>> consumerSupplier, Runnable callback) {
        this.key = key;
        this.consumer = null;
        this.consumerSupplier = consumerSupplier;
        this.callback = callback;
    }

    /**
     * 获取底层 {@link TbQueueConsumer}，必要时按 supplier 惰性创建。
     * <p>
     * 使用双重检查锁定：无锁快速路径返回已有实例；首次创建时加锁，
     * 校验 supplier 与创建结果均非 {@code null}，然后将 supplier 置空。
     * 订阅、停止等操作都应通过本方法拿到同一实例，避免出现「包装存在但未建连」
     * 或「多次创建多个消费者」的情况。
     *
     * @return 可用的底层消费者，不会返回 {@code null}
     * @throws NullPointerException supplier 为 null，或 supplier 返回 null
     */
    public TbQueueConsumer<M> getConsumer() {
        if (consumer == null) {
            synchronized (this) {
                if (consumer == null) {
                    Objects.requireNonNull(consumerSupplier, "consumerSupplier for key [" + key + "] is null");
                    consumer = consumerSupplier.get();
                    Objects.requireNonNull(consumer, "consumer for key [" + key + "] is null");
                    consumerSupplier = null;
                }
            }
        }
        return consumer;
    }

    /**
     * 让底层消费者订阅给定分区集合。
     * <p>
     * 内部会先 {@link #getConsumer()}（可能触发首次创建），再委托
     * {@link TbQueueConsumer#subscribe(Set)}。单消费者模式下分区集合变化时
     * 可反复调用以更新订阅；每分区一消费者模式通常只订阅单个分区。
     *
     * @param partitions 目标分区集合；具体是否允许空集取决于底层实现
     */
    public void subscribe(Set<TopicPartitionInfo> partitions) {
        log.trace("[{}] Subscribing to partitions: {}", key, partitions);
        getConsumer().subscribe(partitions);
    }

    /**
     * 发出停止信号，不阻塞等待线程结束。
     * <p>
     * 委托 {@link TbQueueConsumer#stop()}，使 poll 循环在检查停止标志后退出。
     * 调用方若需要保证资源释放、分区可安全移交，应在之后调用
     * {@link #awaitCompletion()} 或 {@link #awaitCompletion(int)}。
     * 管理器在批量摘除分区时通常对多个 Task 先全部 {@code initiateStop}，
     * 再逐个 {@code awaitCompletion}，以缩短总体停机时间。
     */
    public void initiateStop() {
        log.debug("[{}] Initiating stop", key);
        getConsumer().stop();
    }

    /**
     * 等待消费线程结束，超时时间默认 30 秒。
     * <p>
     * 等价于 {@link #awaitCompletion(int) awaitCompletion(30)}。
     */
    public void awaitCompletion() {
        awaitCompletion(30);
    }

    /**
     * 阻塞等待本任务关联的消费循环 {@link Future} 结束。
     * <p>
     * 仅当 {@link #isRunning()} 为 {@code true} 时才会调用 {@link Future#get()}；
     * 否则直接返回。超时或等待被中断时只打警告日志，不向外抛出，避免拖垮停机流程。
     * 无论成功或失败，方法返回前都会将 {@link #task} 置为 {@code null}，
     * 使后续 {@link #isRunning()} 为 {@code false}（与「是否已从映射中移除」由调用方负责）。
     * <p>
     * 注意：本方法只等待线程退出，不会主动调用 {@link #initiateStop()}；
     * 若循环因未收到停止信号而一直阻塞在 poll，可能一直等到超时。
     *
     * @param timeoutSec 最长等待秒数；{@code <= 0} 表示一直等到 {@link Future#get()} 返回
     */
    public void awaitCompletion(int timeoutSec) {
        log.trace("[{}] Awaiting finish", key);
        if (isRunning()) {
            try {
                if (timeoutSec > 0) {
                    task.get(timeoutSec, TimeUnit.SECONDS);
                } else {
                    task.get();
                }
                log.trace("[{}] Awaited finish", key);
            } catch (Exception e) {
                log.warn("[{}] Failed to await for consumer to stop (timeout {} sec)", key, timeoutSec, e);
            }
            // 无论成功或超时，都清空 Future，避免后续误判仍在运行
            task = null;
        }
    }

    /**
     * 判断消费循环是否仍被视为在运行。
     * <p>
     * 以 {@link #task} 是否非 {@code null} 为准：管理器提交循环后会设置 Future；
     * {@link #awaitCompletion(int)} 结束后会清空。不探测线程是否存活，也不调用
     * {@link Future#isDone()}，因此「Future 已完成但尚未 await」时仍可能返回 {@code true}。
     *
     * @return {@code true} 表示已关联未清理的 {@link Future}；{@code false} 表示未启动或已 await 清理
     */
    public boolean isRunning() {
        return task != null;
    }

}
