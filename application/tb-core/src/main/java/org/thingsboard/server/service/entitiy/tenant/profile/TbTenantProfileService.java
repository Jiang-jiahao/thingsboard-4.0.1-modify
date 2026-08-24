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
package org.thingsboard.server.service.entitiy.tenant.profile;

import org.thingsboard.server.common.data.TenantProfile;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.TenantId;

/**
 * 租户配置（Tenant Profile）业务层契约。
 * <p>
 * 由 TenantProfileController 调用；保存后刷新缓存并按配置同步各租户队列。
 */
public interface TbTenantProfileService {

    /** 保存租户配置，刷新缓存并按新旧配置更新关联租户队列。 */
    TenantProfile save(TenantId tenantId, TenantProfile tenantProfile, TenantProfile oldTenantProfile) throws ThingsboardException;

    /** 删除租户配置。 */
    void delete(TenantId tenantId, TenantProfile tenantProfile) throws ThingsboardException;
}
