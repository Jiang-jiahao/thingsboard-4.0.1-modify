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
package org.thingsboard.server.service.security.system;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.core.AuthenticationException;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.security.UserCredentials;
import org.thingsboard.server.common.data.security.model.UserPasswordPolicy;
import org.thingsboard.server.common.data.security.model.mfa.PlatformTwoFaSettings;
import org.thingsboard.server.dao.exception.DataValidationException;
import org.thingsboard.server.service.security.model.SecurityUser;

/**
 * 平台系统安全策略服务。
 * <p>
 * 登录链路校验密码策略、凭证、双因子失败锁定，并记录登录审计；同时提供租户级站点 Base URL。
 *
 * @see DefaultSystemSecurityService
 */
public interface SystemSecurityService {

    /**
     * 验证密码是否符合密码策略要求
     * @param password
     * @param passwordPolicy
     */
    void validatePasswordByPolicy(String password, UserPasswordPolicy passwordPolicy);

    /**
     * 验证用户密码
     * @param tenantId
     * @param userCredentials
     * @param username
     * @param password
     * @throws AuthenticationException
     */
    void validateUserCredentials(TenantId tenantId, UserCredentials userCredentials, String username, String password) throws AuthenticationException;

    /**
     * 登录后校验双因子结果，失败次数超限则锁定账号。
     */
    void validateTwoFaVerification(SecurityUser securityUser, boolean verificationSuccess, PlatformTwoFaSettings twoFaSettings);

    /**
     * 修改密码时校验新密码策略及是否与历史重复。
     */
    void validatePassword(String password, UserCredentials userCredentials) throws DataValidationException;

    /**
     * 解析邮件/重置链接等场景使用的站点 Base URL。
     */
    String getBaseUrl(TenantId tenantId, CustomerId customerId, HttpServletRequest httpServletRequest);

    /**
     * 记录登录成功日志
     * @param user
     * @param authenticationDetails
     * @param actionType
     * @param e
     */
    void logLoginAction(User user, Object authenticationDetails, ActionType actionType, Exception e);

    /**
     * 记录登录动作（可带 OAuth 等 provider 标识）。
     */
    void logLoginAction(User user, Object authenticationDetails, ActionType actionType, String provider, Exception e);

}
