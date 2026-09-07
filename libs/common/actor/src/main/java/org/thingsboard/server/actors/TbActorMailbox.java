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
package org.thingsboard.server.actors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.msg.MsgType;
import org.thingsboard.server.common.msg.TbActorError;
import org.thingsboard.server.common.msg.TbActorMsg;
import org.thingsboard.server.common.msg.TbActorStopReason;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Actor邮箱实现（核心消息处理单元）
 *
 * <h3>设计职责：</h3>
 * 1. 作为Actor的上下文（实现{@link TbActorCtx}接口）
 * 2. 管理Actor的生命周期（初始化/处理消息/销毁）
 * 3. 实现优先级消息队列处理
 * 4. 提供父子Actor通信机制
 *
 * <h3>关键特性：</h3>
 * - 双优先级消息队列（高优先级/普通优先级）
 * - 异步初始化重试机制
 * - 消息处理吞吐量控制
 * - 线程安全的生命周期管理
 */
@Slf4j
@Getter
@RequiredArgsConstructor
public final class TbActorMailbox implements TbActorCtx {
    private static final boolean HIGH_PRIORITY = true;
    private static final boolean NORMAL_PRIORITY = false;

    private static final boolean FREE = false;
    private static final boolean BUSY = true;

    private static final boolean NOT_READY = false;
    private static final boolean READY = true;

    private final TbActorSystem system;
    private final TbActorSystemSettings settings;
    private final TbActorId selfId;
    private final TbActorRef parentRef;
    private final TbActor actor;
    private final Dispatcher dispatcher;
    private final ConcurrentLinkedQueue<TbActorMsg> highPriorityMsgs = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<TbActorMsg> normalPriorityMsgs = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean busy = new AtomicBoolean(FREE);

    /**
     * actor是否初始化的标识
     */
    private final AtomicBoolean ready = new AtomicBoolean(NOT_READY);

    /**
     * 是否在销毁过程中标识
     */
    private final AtomicBoolean destroyInProgress = new AtomicBoolean();

    /**
     * actor停止原因
     */
    private volatile TbActorStopReason stopReason;

    /**
     * 初始化Actor实例
     *
     * <p>执行流程：
     * 1. 将初始化任务提交到调度器线程池
     * 2. 支持失败重试机制（最多{@code settings.getMaxActorInitAttempts()}次）
     * 3. 根据{@link InitFailureStrategy}决定重试或终止
     */
    public void initActor() {
        dispatcher.getExecutor().execute(() -> tryInit(1));
    }

    /**
     * Actor初始化尝试（带重试逻辑）
     *
     * @param attempt 当前尝试次数（从1开始）
     */
    private void tryInit(int attempt) {
        try {
            log.debug("[{}] Trying to init actor, attempt: {}", selfId, attempt);
            // 检查是否在销毁过程中
            if (!destroyInProgress.get()) {
                // 执行actor的初始化方法
                actor.init(this);
                // 双重检查：初始化后再次确认未进入销毁状态
                if (!destroyInProgress.get()) {
                    // 设置actor已初始化
                    // 双重检查ready状态。虽然还是存在安全问题，但是符合Actor模型的"任其崩溃"哲学，在99%的场景下足够安全
                    ready.set(READY);
                    // 尝试处理队列
                    tryProcessQueue(false);
                }
            }
        } catch (Throwable t) {
            InitFailureStrategy strategy;
            int attemptIdx = attempt + 1;
            if (isUnrecoverable(t)) {
                strategy = InitFailureStrategy.stop();
            } else {
                log.debug("[{}] Failed to init actor, attempt: {}", selfId, attempt, t);
                strategy = actor.onInitFailure(attempt, t);
            }
            if (strategy.isStop() || (settings.getMaxActorInitAttempts() > 0 && attemptIdx > settings.getMaxActorInitAttempts())) {
                log.info("[{}] Failed to init actor, attempt {}, going to stop attempts.", selfId, attempt, t);
                stopReason = TbActorStopReason.INIT_FAILED;
                destroy(t.getCause());
            } else if (strategy.getRetryDelay() > 0) {
                log.info("[{}] Failed to init actor, attempt {}, going to retry in attempts in {}ms", selfId, attempt, strategy.getRetryDelay());
                log.debug("[{}] Error", selfId, t);
                system.getScheduler().schedule(() -> dispatcher.getExecutor().execute(() -> tryInit(attemptIdx)), strategy.getRetryDelay(), TimeUnit.MILLISECONDS);
            } else {
                log.info("[{}] Failed to init actor, attempt {}, going to retry immediately", selfId, attempt);
                log.debug("[{}] Error", selfId, t);
                dispatcher.getExecutor().execute(() -> tryInit(attemptIdx));
            }
        }
    }

    private static boolean isUnrecoverable(Throwable t) {
        if (t instanceof TbActorException && t.getCause() != null) {
            t = t.getCause();
        }
        return t instanceof TbActorError && ((TbActorError) t).isUnrecoverable();
    }

    /**
     * 消息入队处理
     *
     * <p>特殊场景：当收到RULE_NODE_UPDATED_MSG消息且处于销毁状态时：
     * 1. 如果是初始化失败状态，尝试重新初始化Actor
     * 2. 否则通知消息发送方Actor已停止
     *
     * @param msg 待处理消息
     * @param highPriority 是否高优先级
     */
    private void enqueue(TbActorMsg msg, boolean highPriority) {
        if (!destroyInProgress.get()) {
            if (highPriority) {
                highPriorityMsgs.add(msg);
            } else {
                normalPriorityMsgs.add(msg);
            }
            tryProcessQueue(true);
        } else {
            if (highPriority && msg.getMsgType().equals(MsgType.RULE_NODE_UPDATED_MSG)) {
                synchronized (this) {
                    if (stopReason == TbActorStopReason.INIT_FAILED) {
                        destroyInProgress.set(false);
                        stopReason = null;
                        initActor();
                    } else {
                        msg.onTbActorStopped(stopReason);
                    }
                }
            } else {
                msg.onTbActorStopped(stopReason);
            }
        }
    }

    /**
     * 尝试处理消息队列
     *
     * <p>触发条件：
     * 1. Actor处于就绪状态（ready == READY）
     * 2. 消息队列非空或者是新消息触发
     * 3. 当前邮箱空闲（busy == FREE）
     *
     *
     * @param newMsg 是否由新消息触发
     */
    private void tryProcessQueue(boolean newMsg) {
        if (ready.get() == READY) {
            if (newMsg || !highPriorityMsgs.isEmpty() || !normalPriorityMsgs.isEmpty()) {
                if (busy.compareAndSet(FREE, BUSY)) {
                    dispatcher.getExecutor().execute(this::processMailbox);
                } else {
                    log.trace("[{}] MessageBox is busy, new msg: {}", selfId, newMsg);
                }
            } else {
                log.trace("[{}] MessageBox is empty, new msg: {}", selfId, newMsg);
            }
        } else {
            log.trace("[{}] MessageBox is not ready, new msg: {}", selfId, newMsg);
        }
    }

    /**
     * 邮箱核心处理逻辑
     */
    private void processMailbox() {
        boolean noMoreElements = false;
        // 每次处理最多ActorThroughput条消息
        for (int i = 0; i < settings.getActorThroughput(); i++) {
            // 优先处理高优先级队列
            TbActorMsg msg = highPriorityMsgs.poll();
            if (msg == null) {
                msg = normalPriorityMsgs.poll();
            }
            if (msg != null) {
                try {
                    log.trace("[{}] Going to process message: {}", selfId, msg);
                    // 分发给对应的actor处理消息
                    actor.process(msg);
                } catch (TbRuleNodeUpdateException updateException) {
                    // 规则节点更新异常，表示规则节点更新了，需要重新初始化
                    stopReason = TbActorStopReason.INIT_FAILED;
                    // 直接销毁
                    destroy(updateException.getCause());
                } catch (Throwable t) {
                    log.debug("[{}] Failed to process message: {}", selfId, msg, t);
                    // 处理消息异常，执行对应actor的onProcessFailure方法
                    ProcessFailureStrategy strategy = actor.onProcessFailure(msg, t);
                    if (strategy.isStop()) {
                        system.stop(selfId);
                    }
                }
            } else {
                // 表示没有可以处理的元素
                noMoreElements = true;
                break;
            }
        }
        if (noMoreElements) {
            // 没有什么消息，则设置空闲状态
            busy.set(FREE);
            // 空闲的情况直接调用tryProcessQueue，来尝试处理队列的数据，队列有消息再执行processMailbox
            dispatcher.getExecutor().execute(() -> tryProcessQueue(false));
        } else {
            // 表示繁忙状态，直接执行processMailbox，无需再执行tryProcessQueue去判断是否繁忙
            dispatcher.getExecutor().execute(this::processMailbox);
        }
    }

    /**
     * 获取当前Actor的标识
     * @return 当前Actor的ID
     */
    @Override
    public TbActorId getSelf() {
        return selfId;
    }

    /**
     * 向目标Actor发送消息
     * @param target 目标Actor标识
     * @param actorMsg 待发送消息
     */
    @Override
    public void tell(TbActorId target, TbActorMsg actorMsg) {
        system.tell(target, actorMsg);
    }

    /**
     * 广播消息给所有子Actor（普通优先级）
     * @param msg 待广播消息
     */
    @Override
    public void broadcastToChildren(TbActorMsg msg) {
        broadcastToChildren(msg, false);
    }

    /**
     * 广播消息给所有子Actor（可指定优先级）
     * @param msg 待广播消息
     * @param highPriority 是否高优先级
     */
    @Override
    public void broadcastToChildren(TbActorMsg msg, boolean highPriority) {
        system.broadcastToChildren(selfId, msg, highPriority);
    }

    /**
     * 按实体类型广播给子Actor
     * @param msg 待广播消息
     * @param entityType 目标实体类型
     */
    @Override
    public void broadcastToChildrenByType(TbActorMsg msg, EntityType entityType) {
        broadcastToChildren(msg, actorId -> entityType.equals(actorId.getEntityType()));
    }

    /**
     * 条件广播给子Actor
     * @param msg 待广播消息
     * @param childFilter 子Actor过滤条件
     */
    @Override
    public void broadcastToChildren(TbActorMsg msg, Predicate<TbActorId> childFilter) {
        system.broadcastToChildren(selfId, childFilter, msg);
    }

    /**
     * 过滤子Actor
     * @param childFilter 过滤条件
     * @return 符合条件的子Actor ID列表
     */
    @Override
    public List<TbActorId> filterChildren(Predicate<TbActorId> childFilter) {
        return system.filterChildren(selfId, childFilter);
    }

    /**
     * 停止目标Actor
     * @param target 目标Actor标识
     */
    @Override
    public void stop(TbActorId target) {
        system.stop(target);
    }

    /**
     * 获取对应id的子actor，如果不存在则创建子Actor
     *
     * <p>执行逻辑：
     * 1. 检查目标Actor是否已存在
     * 2. 当Actor不存在且满足创建条件时，创建新的子Actor
     *
     * @param actorId 目标Actor标识
     * @param dispatcher 调度器提供函数
     * @param creator Actor创建器提供函数
     * @param createCondition 创建条件提供函数
     * @return 已存在或新建的Actor引用
     */
    @Override
    public TbActorRef getOrCreateChildActor(TbActorId actorId, Supplier<String> dispatcher, Supplier<TbActorCreator> creator, Supplier<Boolean> createCondition) {
        TbActorRef actorRef = system.getActor(actorId);
        if (actorRef == null && createCondition.get()) {
            return system.createChildActor(dispatcher.get(), creator.get(), selfId);
        } else {
            return actorRef;
        }
    }

    /**
     * 销毁Actor（清理资源）
     *
     * <p>执行流程：
     * 1. 标记销毁状态（destroyInProgress = true）
     * 2. 调用Actor的destroy()方法
     * 3. 通知所有待处理消息（onTbActorStopped）
     *
     * @param cause 销毁原因（异常信息）
     */
    public void destroy(Throwable cause) {
        if (stopReason == null) {
            stopReason = TbActorStopReason.STOPPED;
        }
        destroyInProgress.set(true);
        dispatcher.getExecutor().execute(() -> {
            try {
                ready.set(NOT_READY);
                actor.destroy(stopReason, cause);
                highPriorityMsgs.forEach(msg -> msg.onTbActorStopped(stopReason));
                normalPriorityMsgs.forEach(msg -> msg.onTbActorStopped(stopReason));
            } catch (Throwable t) {
                log.warn("[{}] Failed to destroy actor: ", selfId, t);
            }
        });
    }

    /**
     * 获取当前Actor标识
     * @return 当前Actor ID
     */
    @Override
    public TbActorId getActorId() {
        return selfId;
    }

    /**
     * 向当前Actor发送消息（普通优先级）
     * @param actorMsg 待发送消息
     */
    @Override
    public void tell(TbActorMsg actorMsg) {
        enqueue(actorMsg, NORMAL_PRIORITY);
    }

    /**
     * 向当前Actor发送高优先级消息
     * @param actorMsg 待发送消息
     */
    @Override
    public void tellWithHighPriority(TbActorMsg actorMsg) {
        enqueue(actorMsg, HIGH_PRIORITY);
    }

}
