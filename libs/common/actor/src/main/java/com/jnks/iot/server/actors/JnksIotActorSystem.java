package com.jnks.iot.server.actors;

import com.jnks.iot.server.common.msg.JnksIotActorMsg;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

public interface JnksIotActorSystem {

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
    JnksIotActorRef getActor(JnksIotActorId actorId);

    /**
     * 创建一个根actor（也就是父actor）
     * @param dispatcherId 调度器唯一id
     * @param creator actor创建器
     * @return actor
     */
    JnksIotActorRef createRootActor(String dispatcherId, JnksIotActorCreator creator);

    /**
     * 创建一个子actor
     * @param dispatcherId 调度器唯一id
     * @param creator actor创建器
     * @param parent parentActorId
     * @return actor
     */
    JnksIotActorRef createChildActor(String dispatcherId, JnksIotActorCreator creator, JnksIotActorId parent);

    /**
     * 给指定actorId的actor发送消息
     * @param target actorId
     * @param actorMsg actor消息
     */
    void tell(JnksIotActorId target, JnksIotActorMsg actorMsg);

    /**
     * 给指定actorId的actor发送高优先级消息（该消息高优先级被指定actor处理）
     * @param target actorId
     * @param actorMsg actor消息
     */
    void tellWithHighPriority(JnksIotActorId target, JnksIotActorMsg actorMsg);

    /**
     * 停止指定actorId的actor及其子actor
     * @param actorRef actorId
     */
    void stop(JnksIotActorRef actorRef);

    /**
     * 停止指定actorId的actor及其子actor（实际实现方法）
     * @param actorId actorId
     */
    void stop(JnksIotActorId actorId);

    /**
     * 停止actor系统
     */
    void stop();

    /**
     * 给指定actorId的actor的子级actor发送消息
     * @param parent actorId
     * @param msg actor消息
     */
    void broadcastToChildren(JnksIotActorId parent, JnksIotActorMsg msg);

    /**
     * 给指定actorId的actor的子级actor发送消息（可指定优先级）
     * @param parent actorId
     * @param msg actor消息
     * @param highPriority 是否高优先级
     */
    void broadcastToChildren(JnksIotActorId parent, JnksIotActorMsg msg, boolean highPriority);

    /**
     * 给指定actorId的actor的部分子级actor发送消息（可指定子actor过滤器）
     * @param parent 父级actorId
     * @param childFilter 子级actorId过滤器
     * @param msg actor消息
     */
    void broadcastToChildren(JnksIotActorId parent, Predicate<JnksIotActorId> childFilter, JnksIotActorMsg msg);

    /**
     * 获取指定actorId的actor的子级actorId
     * @param parent 父级actorId
     * @param childFilter 子级actor过滤器
     * @return list 过滤后的子级actorId集合
     */
    List<JnksIotActorId> filterChildren(JnksIotActorId parent, Predicate<JnksIotActorId> childFilter);
}
