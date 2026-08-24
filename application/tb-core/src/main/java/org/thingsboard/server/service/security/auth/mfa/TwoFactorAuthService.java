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
package org.thingsboard.server.service.security.auth.mfa;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.security.model.mfa.account.TwoFaAccountConfig;
import org.thingsboard.server.common.data.security.model.mfa.provider.TwoFaProviderType;
import org.thingsboard.server.service.security.model.SecurityUser;

/**
 * 双因子认证服务。
 * <p>
 * 登录第二步：按平台/账号配置生成与校验验证码，并做发送/校验频控。
 *
 * @see DefaultTwoFactorAuthService
 */
public interface TwoFactorAuthService {

    /**
     * 用户是否已配置至少一种 2FA。
     */
    boolean isTwoFaEnabled(TenantId tenantId, UserId userId);

    /**
     * 检查指定提供方在租户下是否可用。
     */
    void checkProvider(TenantId tenantId, TwoFaProviderType providerType) throws ThingsboardException;


    /**
     * 按提供方类型准备验证码（如发邮件/短信）。
     */
    void prepareVerificationCode(SecurityUser user, TwoFaProviderType providerType, boolean checkLimits) throws Exception;

    /**
     * 按账号配置准备验证码。
     */
    void prepareVerificationCode(SecurityUser user, TwoFaAccountConfig accountConfig, boolean checkLimits) throws ThingsboardException;


    /**
     * 按提供方类型校验验证码。
     */
    boolean checkVerificationCode(SecurityUser user, TwoFaProviderType providerType, String verificationCode, boolean checkLimits) throws ThingsboardException;

    /**
     * 按账号配置校验验证码。
     */
    boolean checkVerificationCode(SecurityUser user, String verificationCode, TwoFaAccountConfig accountConfig, boolean checkLimits) throws ThingsboardException;


    /**
     * 为用户生成新的 2FA 账号配置（如 TOTP 密钥）。
     */
    TwoFaAccountConfig generateNewAccountConfig(User user, TwoFaProviderType providerType) throws ThingsboardException;

}
