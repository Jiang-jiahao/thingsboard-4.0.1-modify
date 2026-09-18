package org.thingsboard.server.queue.discovery;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.With;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.queue.Queue;
import org.thingsboard.server.common.msg.queue.ServiceType;

/**
 * 哪个服务、哪条队列名、哪个租户”这三个维度组合成一个全局唯一的 key
 * TB_CORE + main + tenant_A	租户A的核心默认队列
 * TB_RULE_ENGINE + highPriority + tenant_B	租户B的规则引擎高优队列
 * TB_TRANSPORT + main + system	系统级传输默认队列
 */
@Data
@AllArgsConstructor
public class QueueKey {

    // 服务类型
    private final ServiceType type;

    // 队列名
    @With
    private final String queueName;

    // 租户id
    private final TenantId tenantId;

    public QueueKey(ServiceType type, Queue queue) {
        this.type = type;
        this.queueName = queue.getName();
        this.tenantId = queue.getTenantId();
    }

    public QueueKey(ServiceType type, QueueRoutingInfo queueRoutingInfo) {
        this.type = type;
        this.queueName = queueRoutingInfo.getQueueName();
        this.tenantId = queueRoutingInfo.getTenantId();
    }

    public QueueKey(ServiceType type, TenantId tenantId) {
        this.type = type;
        this.queueName = DataConstants.MAIN_QUEUE_NAME;
        this.tenantId = tenantId != null ? tenantId : TenantId.SYS_TENANT_ID;
    }

    public QueueKey(ServiceType type) {
        this.type = type;
        this.queueName = DataConstants.MAIN_QUEUE_NAME;
        this.tenantId = TenantId.SYS_TENANT_ID;
    }

    public QueueKey(ServiceType type, String queueName) {
        this.type = type;
        this.queueName = queueName;
        this.tenantId = TenantId.SYS_TENANT_ID;
    }

    @Override
    public String toString() {
        return "QK(" + queueName + "," + type + "," +
                (TenantId.SYS_TENANT_ID.equals(tenantId) ? "system" : tenantId) +
                ')';
    }
}
