package org.thingsboard.server.service.cf;

import org.thingsboard.server.common.data.cf.CalculatedField;
import org.thingsboard.server.common.data.cf.CalculatedFieldLink;
import org.thingsboard.server.common.data.id.CalculatedFieldId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;

import java.util.List;

public interface CalculatedFieldCache {

    CalculatedField getCalculatedField(CalculatedFieldId calculatedFieldId);

    List<CalculatedField> getCalculatedFieldsByEntityId(EntityId entityId);

    List<CalculatedFieldLink> getCalculatedFieldLinksByEntityId(EntityId entityId);

    <T> T getCalculatedFieldCtx(CalculatedFieldId calculatedFieldId);

    <T> List<T> getCalculatedFieldCtxsByEntityId(EntityId entityId);

    void addCalculatedField(TenantId tenantId, CalculatedFieldId calculatedFieldId);

    void updateCalculatedField(TenantId tenantId, CalculatedFieldId calculatedFieldId);

    void evict(CalculatedFieldId calculatedFieldId);

}
