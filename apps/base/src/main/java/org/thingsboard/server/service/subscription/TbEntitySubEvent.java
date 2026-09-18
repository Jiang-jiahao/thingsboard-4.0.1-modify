package org.thingsboard.server.service.subscription;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;

/**
 * Information about the local websocket subscriptions.
 */
@Builder
@Data
public class TbEntitySubEvent {

    private final TenantId tenantId;
    private final EntityId entityId;
    /**
     * 组件生命周期事件
     */
    private final ComponentLifecycleEvent type;
    /**
     * 订阅状态信息
     */
    private final TbSubscriptionsInfo info;
    private final int seqNumber;

    public boolean hasTsOrAttrSub() {
        return info != null && (info.tsAllKeys || info.attrAllKeys || info.tsKeys != null || info.attrKeys != null);
    }
}
