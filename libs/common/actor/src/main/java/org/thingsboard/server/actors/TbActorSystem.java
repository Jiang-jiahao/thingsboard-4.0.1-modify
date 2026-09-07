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

import org.thingsboard.server.common.msg.TbActorMsg;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

public interface TbActorSystem {

    ScheduledExecutorService getScheduler();

    /**
     * 创建一个调度器
     * 如果已经存在对应 dispatcherId的调度器，则抛出异常。不存在则创建一个调度器
     * @param dispatcherId 调度器唯一id
     * @param executor 调度器的线程池
     */
    void createDispatcher(String dispatcherId, ExecutorService executor);

    /**
     * 销毁一个调度器
     * 如果不存在对应 dispatcherId的调度器，则抛出异常。存在则销毁一个调度器，并停止其内部的线程池
     * @param dispatcherId 调度器唯一id
     */
    void destroyDispatcher(String dispatcherId);

    /**
     * 获取指定id的 actor
     * @param actorId actor唯一id
     * @return actor
     */
    TbActorRef getActor(TbActorId actorId);

    /**
     * 创建一个根actor（也就是父actor）
     * @param dispatcherId 调度器唯一id
     * @param creator actor创建器
     * @return actor
     */
    TbActorRef createRootActor(String dispatcherId, TbActorCreator creator);

    /**
     * 创建一个子actor
     * @param dispatcherId 调度器唯一id
     * @param creator actor创建器
     * @param parent parentActorId
     * @return actor
     */
    TbActorRef createChildActor(String dispatcherId, TbActorCreator creator, TbActorId parent);

    /**
     * 给指定actorId的actor发送消息
     * @param target actorId
     * @param actorMsg actor消息
     */
    void tell(TbActorId target, TbActorMsg actorMsg);

    /**
     * 给指定actorId的actor发送高优先级消息（该消息高优先级被指定actor处理）
     * @param target actorId
     * @param actorMsg actor消息
     */
    void tellWithHighPriority(TbActorId target, TbActorMsg actorMsg);

    /**
     * 停止指定actorId的actor及其子actor
     * @param actorRef actorId
     */
    void stop(TbActorRef actorRef);

    /**
     * 停止指定actorId的actor及其子actor（实际实现方法）
     * @param actorId actorId
     */
    void stop(TbActorId actorId);

    /**
     * 停止actor系统
     */
    void stop();

    /**
     * 给指定actorId的actor的子级actor发送消息
     * @param parent actorId
     * @param msg actor消息
     */
    void broadcastToChildren(TbActorId parent, TbActorMsg msg);

    /**
     * 给指定actorId的actor的子级actor发送消息（可指定优先级）
     * @param parent actorId
     * @param msg actor消息
     * @param highPriority 是否高优先级
     */
    void broadcastToChildren(TbActorId parent, TbActorMsg msg, boolean highPriority);

    /**
     * 给指定actorId的actor的部分子级actor发送消息（可指定子actor过滤器）
     * @param parent 父级actorId
     * @param childFilter 子级actorId过滤器
     * @param msg actor消息
     */
    void broadcastToChildren(TbActorId parent, Predicate<TbActorId> childFilter, TbActorMsg msg);

    /**
     * 获取指定actorId的actor的子级actorId
     * @param parent 父级actorId
     * @param childFilter 子级actor过滤器
     * @return list 过滤后的子级actorId集合
     */
    List<TbActorId> filterChildren(TbActorId parent, Predicate<TbActorId> childFilter);
}
