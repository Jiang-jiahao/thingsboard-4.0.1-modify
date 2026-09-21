package com.jnks.iot.server.service.subscription;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;

/**
 * Information about the local websocket subscriptions.
 */
@Builder
@Data
public class JnksIotEntitySubEvent {

    private final TenantId tenantId;
    private final EntityId entityId;
    /**
     * 组件生命周期事件
     */
    private final ComponentLifecycleEvent type;
    /**
     * 订阅状态信息
     */
    private final JnksIotSubscriptionsInfo info;
    private final int seqNumber;

    public boolean hasTsOrAttrSub() {
        return info != null && (info.tsAllKeys || info.attrAllKeys || info.tsKeys != null || info.attrKeys != null);
    }
}
