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
package org.thingsboard.server.service.security.permission;

import org.thingsboard.server.common.data.HasTenantId;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.service.security.model.SecurityUser;

/**
 * REST / WebSocket 访问控制入口。
 * <p>
 * 按用户角色（系统管理员 / 租户管理员 / 客户用户）选择权限表，校验资源级或实体级操作。
 *
 * @see DefaultAccessControlService
 */
public interface AccessControlService {

    /**
     * 校验用户是否具备某资源上的操作权限（不绑定具体实体）。
     */
    void checkPermission(SecurityUser user, Resource resource, Operation operation) throws ThingsboardException;

    /**
     * 校验用户对指定实体的操作权限。
     */
    <I extends EntityId, T extends HasTenantId> void checkPermission(SecurityUser user, Resource resource, Operation operation, I entityId, T entity) throws ThingsboardException;

}
