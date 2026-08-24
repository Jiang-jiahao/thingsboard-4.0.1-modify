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
package org.thingsboard.server.service.entitiy.user;

import jakarta.servlet.http.HttpServletRequest;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.UserActivationLink;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;

/**
 * 用户业务层契约：保存（可选发激活邮件）、删除与获取激活链接。
 * <p>
 * 由 UserController 调用；实现类委托 User DAO，写审计日志，新建时可发邮件。
 */
public interface TbUserService {

    /** 保存用户；新建且 {@code sendActivationMail} 为 true 时发送激活邮件。 */
    User save(TenantId tenantId, CustomerId customerId, User tbUser, boolean sendActivationMail, HttpServletRequest request, User user) throws ThingsboardException;

    /** 删除用户。 */
    void delete(TenantId tenantId, CustomerId customerId, User user, User responsibleUser) throws ThingsboardException;

    /** 生成尚未激活用户的激活链接。 */
    UserActivationLink getActivationLink(TenantId tenantId, CustomerId customerId, UserId userId, HttpServletRequest request) throws ThingsboardException;

}
