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
package org.thingsboard.server.queue.discovery;

import lombok.Data;
import org.thingsboard.server.common.data.id.QueueId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.queue.Queue;
import org.thingsboard.server.gen.transport.TransportProtos.GetQueueRoutingInfoResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.QueueUpdateMsg;

import java.util.UUID;

/**
 * 规则引擎队列的路由快照（只含路由所需字段，不含业务配置全文）。
 * <p>
 * 供 {@link HashPartitionService} 等组件使用：据此构造 {@link QueueKey}，
 * 并登记 topic、分区数、是否向全部分区复制消息。
 * <p>
 * 与 {@link QueueKey} 的对应关系：
 * <ul>
 *   <li>{@code tenantId} + {@code queueName} + 固定 type=TB_RULE_ENGINE → QueueKey</li>
 *   <li>{@code queueTopic} / {@code partitions} → partitionTopicsMap / partitionSizesMap</li>
 *   <li>{@code duplicateMsgToAllPartitions} → HashPartitionService.QueueConfig</li>
 * </ul>
 * 数据可来自本机 {@link Queue} 实体，或跨服务的 Protobuf（查询响应 / 队列变更通知）。
 */
@Data
public class QueueRoutingInfo {

    /** 队列所属租户；系统共享队列为 SYS_TENANT_ID，隔离租户为真实租户 ID */
    private final TenantId tenantId;

    /** 队列实体 ID（数据库中的 Queue 主键） */
    private final QueueId queueId;

    /** 逻辑队列名，如 Main、HighPriority；参与 QueueKey.queueName */
    private final String queueName;

    /** 该队列对应的消息 topic（逻辑名，实际发送前可能再经 TopicService 加前缀） */
    private final String queueTopic;

    /** 分区数量；发送时对 entityId 哈希取模落入 [0, partitions) */
    private final int partitions;

    /**
     * 是否把同一条消息复制投递到该队列的全部分区。
     * 为 true 时 {@link HashPartitionService#resolveAll} 会返回多个 TopicPartitionInfo。
     */
    private final boolean duplicateMsgToAllPartitions;

    /** 从 DAO/领域对象 {@link Queue} 构造（本机已持有完整队列实体时） */
    public QueueRoutingInfo(Queue queue) {
        this.tenantId = queue.getTenantId();
        this.queueId = queue.getId();
        this.queueName = queue.getName();
        this.queueTopic = queue.getTopic();
        this.partitions = queue.getPartitions();
        this.duplicateMsgToAllPartitions = queue.isDuplicateMsgToAllPartitions();
    }

    /** 从跨服务查询响应构造（例如 Transport 向 Core 拉取全部队列路由信息） */
    public QueueRoutingInfo(GetQueueRoutingInfoResponseMsg routingInfo) {
        this.tenantId = TenantId.fromUUID(new UUID(routingInfo.getTenantIdMSB(), routingInfo.getTenantIdLSB()));
        this.queueId = new QueueId(new UUID(routingInfo.getQueueIdMSB(), routingInfo.getQueueIdLSB()));
        this.queueName = routingInfo.getQueueName();
        this.queueTopic = routingInfo.getQueueTopic();
        this.partitions = routingInfo.getPartitions();
        this.duplicateMsgToAllPartitions = routingInfo.hasDuplicateMsgToAllPartitions() && routingInfo.getDuplicateMsgToAllPartitions();
    }

    /** 从队列创建/更新通知构造（集群内广播队列变更时） */
    public QueueRoutingInfo(QueueUpdateMsg queueUpdateMsg) {
        this.tenantId = TenantId.fromUUID(new UUID(queueUpdateMsg.getTenantIdMSB(), queueUpdateMsg.getTenantIdLSB()));
        this.queueId = new QueueId(new UUID(queueUpdateMsg.getQueueIdMSB(), queueUpdateMsg.getQueueIdLSB()));
        this.queueName = queueUpdateMsg.getQueueName();
        this.queueTopic = queueUpdateMsg.getQueueTopic();
        this.partitions = queueUpdateMsg.getPartitions();
        this.duplicateMsgToAllPartitions = queueUpdateMsg.hasDuplicateMsgToAllPartitions() && queueUpdateMsg.getDuplicateMsgToAllPartitions();
    }

}
