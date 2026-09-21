package com.jnks.iot.server.service.ttl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.queue.discovery.PartitionService;


/**
 * TTL 清理服务抽象基类：判断当前节点是否持有系统租户的 Core 分区。
 * <p>
 * <b>职责：</b>集群下仅系统分区所属节点执行实际 DROP/DELETE，其余节点只清本地分区缓存。
 * <p>
 * <b>触发方式：</b>由子类定时任务调用 {@link #isSystemTenantPartitionMine()}。
 */
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractCleanUpService {

    private final PartitionService partitionService;

    /** 当前节点是否负责系统租户的 Core 分区。 */
    protected boolean isSystemTenantPartitionMine() {
        return partitionService.resolve(ServiceType.JNKS_IOT_CORE, TenantId.SYS_TENANT_ID, TenantId.SYS_TENANT_ID).isMyPartition();
    }

}
