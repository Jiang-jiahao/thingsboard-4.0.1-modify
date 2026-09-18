package org.thingsboard.server.service.queue;

import org.springframework.context.ApplicationListener;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;

/**
 * Core 主队列消费服务标记接口。
 * <p>
 * 实现类订阅本节点负责的 Core Topic 分区，将 {@code ToCoreMsg} / 通知消息分发到设备状态、订阅、RPC 等组件。
 * 分区变更通过 {@link PartitionChangeEvent} 驱动。
 *
 * @see DefaultTbCoreConsumerService
 */
public interface TbCoreConsumerService extends ApplicationListener<PartitionChangeEvent> {

}
