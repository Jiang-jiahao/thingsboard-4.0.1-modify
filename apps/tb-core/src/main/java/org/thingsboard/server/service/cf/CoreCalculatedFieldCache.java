package org.thingsboard.server.service.cf;

import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.cf.CalculatedField;
import org.thingsboard.server.common.data.cf.CalculatedFieldLink;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;

import java.util.Collections;
import java.util.List;

/**
 * tb-core 不持有计算字段运行时上下文（由 tb-rule-engine 处理）。
 * 生命周期消息到达时忽略本地缓存，真正的计算在 RE 侧完成。
 */
@Service
public class CoreCalculatedFieldCache implements CalculatedFieldCache {

    @Override
    public CalculatedField getCalculatedField(CalculatedFieldId calculatedFieldId) {
        return null;
    }

    @Override
    public List<CalculatedField> getCalculatedFieldsByEntityId(EntityId entityId) {
        return Collections.emptyList();
    }

    @Override
    public List<CalculatedFieldLink> getCalculatedFieldLinksByEntityId(EntityId entityId) {
        return Collections.emptyList();
    }

    @Override
    public <T> T getCalculatedFieldCtx(CalculatedFieldId calculatedFieldId) {
        return null;
    }

    @Override
    public <T> List<T> getCalculatedFieldCtxsByEntityId(EntityId entityId) {
        return Collections.emptyList();
    }

    @Override
    public void addCalculatedField(TenantId tenantId, CalculatedFieldId calculatedFieldId) {
    }

    @Override
    public void updateCalculatedField(TenantId tenantId, CalculatedFieldId calculatedFieldId) {
    }

    @Override
    public void evict(CalculatedFieldId calculatedFieldId) {
    }

}
