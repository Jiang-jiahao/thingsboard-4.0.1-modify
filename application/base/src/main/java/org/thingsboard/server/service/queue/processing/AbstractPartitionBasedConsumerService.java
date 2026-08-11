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
package org.thingsboard.server.service.queue.processing;

import jakarta.annotation.PostConstruct;
import org.springframework.context.ApplicationEventPublisher;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.dao.tenant.TbTenantProfileCache;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.util.AfterStartUp;
import org.thingsboard.server.service.apiusage.TbApiUsageStateService;
import org.thingsboard.server.service.cf.CalculatedFieldCache;
import org.thingsboard.server.service.profile.TbAssetProfileCache;
import org.thingsboard.server.service.profile.TbDeviceProfileCache;
import org.thingsboard.server.service.security.auth.jwt.settings.JwtSettingsService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 在 {@link AbstractConsumerService} 之上增加「分区驱动消费」启动时序保护的抽象基类。
 * <p>
 * 适用场景：主业务队列必须等本服务自定义启动逻辑（{@link #onStartUp()}）就绪后，
 * 才能安全地根据 {@link PartitionChangeEvent} 去订阅/更新分区。典型子类包括
 * Rule Engine、Calculated Field 等消费服务。
 * <p>
 * 要解决的竞态：Discovery 可能在 {@link #afterStartUp()} 完成前就发布分区变更；
 * 若此时直接调 {@link #onPartitionChangeEvent}，子类内部的消费者管理器可能尚未创建或未就绪。
 * 因此本类在 {@code started == false} 时将事件缓存在 {@link #pendingEvents}，
 * 待 {@link #afterStartUp()} 中执行完 {@link #onStartUp()} 后再依次回放，并置 {@code started = true}。
 * <p>
 * 与直接继承 {@link AbstractConsumerService} 的实现（如部分 Core 消费服务）相比：
 * 本类把「分区事件处理」收敛到抽象方法 {@link #onPartitionChangeEvent}，
 * 并用启动锁保证「先启动、再应用分区」的顺序。
 *
 * @param <N> 通知队列 Protobuf 消息类型，透传给父类
 * @see AbstractConsumerService
 * @see PartitionChangeEvent
 */
public abstract class AbstractPartitionBasedConsumerService<N extends com.google.protobuf.GeneratedMessageV3> extends AbstractConsumerService<N> {

    /**
     * 保护 {@link #started} 与 {@link #pendingEvents} 的互斥锁。
     * <p>
     * 分区事件线程与 {@link #afterStartUp()} 可能并发：一边缓冲事件，一边回放并翻转 started，
     * 必须在同一把锁下完成「判 started → 入队 / 回放 → 置 started」以避免丢失或重复处理。
     */
    private final Lock startupLock = new ReentrantLock();

    /**
     * 服务是否已完成启动回放阶段。
     * <p>
     * {@code false}：分区事件只入 {@link #pendingEvents}；
     * {@code true}：分区事件直接交给 {@link #onPartitionChangeEvent}。
     * 使用 {@code volatile} 保证无锁快速路径上的可见性；写入仍在 {@link #startupLock} 内。
     */
    private volatile boolean started = false;

    /**
     * 启动完成前收到的分区变更事件缓冲。
     * <p>
     * 仅在 {@code started == false} 时写入；{@link #afterStartUp()} 回放完毕后置为 {@code null}，
     * 释放列表并防止后续误用。
     */
    private List<PartitionChangeEvent> pendingEvents = new ArrayList<>();

    /**
     * 将依赖转交给 {@link AbstractConsumerService}；本类不额外持有业务依赖。
     */
    public AbstractPartitionBasedConsumerService(ActorSystemContext actorContext,
                                                 TbTenantProfileCache tenantProfileCache,
                                                 TbDeviceProfileCache deviceProfileCache,
                                                 TbAssetProfileCache assetProfileCache,
                                                 CalculatedFieldCache calculatedFieldCache,
                                                 TbApiUsageStateService apiUsageStateService,
                                                 PartitionService partitionService,
                                                 ApplicationEventPublisher eventPublisher,
                                                 Optional<JwtSettingsService> jwtSettingsService) {
        super(actorContext, tenantProfileCache, deviceProfileCache, assetProfileCache, calculatedFieldCache, apiUsageStateService, partitionService, eventPublisher, jwtSettingsService);
    }

    /**
     * Bean 初始化：用子类提供的 {@link #getPrefix()} 调用父类 {@link AbstractConsumerService#init(String)}，
     * 创建公共线程池与通知消费者。子类若还需构建主通道消费者，应在自身 {@link #onStartUp()} 或
     * 覆盖本方法（先 {@code super.init()}）中完成。
     */
    @PostConstruct
    public void init() {
        super.init(getPrefix());
    }

    /**
     * 应用就绪后的启动入口（{@link AfterStartUp#REGULAR_SERVICE}）。
     * <p>
     * 顺序固定为：
     * <ol>
     *   <li>{@code super.afterStartUp()}：启动父类通知消费者；</li>
     *   <li>{@link #onStartUp()}：子类创建/启动分区相关的主消费组件；</li>
     *   <li>在 {@link #startupLock} 下回放 {@link #pendingEvents}，逐个调用
     *       {@link #onPartitionChangeEvent}（单条失败只记日志，继续后续事件）；</li>
     *   <li>置 {@code started = true}，并清空缓冲引用。</li>
     * </ol>
     * 之后再到达的分区事件走 {@link #onTbApplicationEvent} 的快速路径，不再缓冲。
     */
    @AfterStartUp(order = AfterStartUp.REGULAR_SERVICE)
    @Override
    public void afterStartUp() {
        super.afterStartUp();
        onStartUp();
        startupLock.lock();
        try {
            for (PartitionChangeEvent partitionChangeEvent : pendingEvents) {
                log.info("Handling partition change event: {}", partitionChangeEvent);
                try {
                    onPartitionChangeEvent(partitionChangeEvent);
                } catch (Throwable t) {
                    log.error("Failed to handle partition change event: {}", partitionChangeEvent, t);
                }
            }
            started = true;
            pendingEvents = null;
        } finally {
            startupLock.unlock();
        }
    }

    /**
     * 接收已通过父类 {@code filterTbApplicationEvent} 过滤的分区变更事件。
     * <p>
     * 若尚未 {@code started}：加锁双重检查后写入 {@link #pendingEvents} 并返回
     * （避免与 {@link #afterStartUp()} 回放交叉时丢事件或提前处理）。
     * 若已启动：直接委托 {@link #onPartitionChangeEvent}。
     *
     * @param event 本服务类型相关的分区变更事件
     */
    @Override
    protected void onTbApplicationEvent(PartitionChangeEvent event) {
        log.debug("Received partition change event: {}", event);
        if (!started) {
            startupLock.lock();
            try {
                if (!started) {
                    log.debug("App not started yet, storing event for later: {}", event);
                    pendingEvents.add(event);
                    return;
                }
            } finally {
                startupLock.unlock();
            }
        }
        log.info("Handling partition change event: {}", event);
        onPartitionChangeEvent(event);
    }

    /**
     * 子类自定义启动逻辑：在通知消费者已启动之后、分区事件开始生效之前执行。
     * <p>
     * 典型工作：构建主队列 {@code MainQueueConsumerManager} / 按队列注册消费者等，
     * 使后续 {@link #onPartitionChangeEvent} 可以安全地 {@code update(partitions)}。
     */
    protected abstract void onStartUp();

    /**
     * 应用分区变更：由子类根据事件中的 QueueKey / 分区集合更新各自的消费者订阅。
     * <p>
     * 可能在启动回放阶段被连续调用多次，也可能在运行期由 Discovery 重算后再次触发。
     *
     * @param event 分区变更事件（已保证服务类型匹配）
     */
    protected abstract void onPartitionChangeEvent(PartitionChangeEvent event);

    /**
     * @return 传给父类 {@link AbstractConsumerService#init(String)} 的线程名前缀
     *         （如 {@code "tb-rule-engine"}）
     */
    protected abstract String getPrefix();

}
