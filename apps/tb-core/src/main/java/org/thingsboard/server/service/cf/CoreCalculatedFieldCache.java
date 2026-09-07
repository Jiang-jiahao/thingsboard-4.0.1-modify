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
