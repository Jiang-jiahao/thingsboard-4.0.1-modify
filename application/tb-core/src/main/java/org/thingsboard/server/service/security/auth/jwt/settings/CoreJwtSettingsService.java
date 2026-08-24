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
package org.thingsboard.server.service.security.auth.jwt.settings;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.cluster.TbClusterService;
import org.thingsboard.server.common.data.AdminSettings;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.plugin.ComponentLifecycleEvent;
import org.thingsboard.server.common.data.security.model.JwtSettings;
import org.thingsboard.server.dao.settings.AdminSettingsService;
import org.thingsboard.server.service.security.model.token.JwtTokenFactory;

import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

import static org.thingsboard.server.service.security.model.token.JwtTokenFactory.KEY_LENGTH;

/**
 * Core 节点 JWT 设置服务。
 * <p>
 * 将签发密钥、过期时间等持久化到系统租户 AdminSettings；保存后广播租户生命周期，使集群内 {@link JwtTokenFactory} 重新加载。
 * 仅 monolith / tb-core 生效。YAML/ENV 中的 JWT 参数在库中有记录后被忽略。
 *
 * @see JwtSettingsService
 */
@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnExpression("'${service.type:null}'=='monolith' || '${service.type:null}'=='tb-core'")
public class CoreJwtSettingsService implements JwtSettingsService {

    private final AdminSettingsService adminSettingsService;
    private final Optional<TbClusterService> tbClusterService;
    private final JwtSettingsValidator jwtSettingsValidator;
    private final Optional<JwtTokenFactory> jwtTokenFactory;

    private volatile JwtSettings jwtSettings = null; //lazy init

    /**
     * 校验并保存 JWT 设置，广播集群刷新后返回最新值。
     */
    @Override
    public JwtSettings saveJwtSettings(JwtSettings jwtSettings) {
        jwtSettingsValidator.validate(jwtSettings);
        final AdminSettings adminJwtSettings = mapJwtToAdminSettings(jwtSettings);
        final AdminSettings existedSettings = adminSettingsService.findAdminSettingsByKey(TenantId.SYS_TENANT_ID, ADMIN_SETTINGS_JWT_KEY);
        if (existedSettings != null) {
            adminJwtSettings.setId(existedSettings.getId());
        }

        log.info("Saving new JWT admin settings. From this moment, the JWT parameters from YAML and ENV will be ignored");
        adminSettingsService.saveAdminSettings(TenantId.SYS_TENANT_ID, adminJwtSettings);

        tbClusterService.ifPresent(cs -> cs.broadcastEntityStateChangeEvent(TenantId.SYS_TENANT_ID, TenantId.SYS_TENANT_ID, ComponentLifecycleEvent.UPDATED));
        return reloadJwtSettings();
    }

    /**
     * 强制从库重载并刷新 {@link JwtTokenFactory}。
     */
    @Override
    public JwtSettings reloadJwtSettings() {
        log.trace("Executing reloadJwtSettings");
        var settings = getJwtSettings(true);
        jwtTokenFactory.ifPresent(JwtTokenFactory::reload);
        return settings;
    }

    /**
     * 返回缓存的 JWT 设置（懒加载）。
     */
    @Override
    public JwtSettings getJwtSettings() {
        log.trace("Executing getJwtSettings");
        return getJwtSettings(false);
    }

    /**
     * 读取 JWT 设置；{@code forceReload} 为 true 时无视缓存。
     */
    public JwtSettings getJwtSettings(boolean forceReload) {
        if (this.jwtSettings == null || forceReload) {
            synchronized (this) {
                if (this.jwtSettings == null || forceReload) {
                    jwtSettings = getJwtSettingsFromDb();
                }
            }
        }
        return this.jwtSettings;
    }

    private JwtSettings getJwtSettingsFromDb() {
        AdminSettings adminJwtSettings = adminSettingsService.findAdminSettingsByKey(TenantId.SYS_TENANT_ID, ADMIN_SETTINGS_JWT_KEY);
        return adminJwtSettings != null ? mapAdminToJwtSettings(adminJwtSettings) : null;
    }

    private JwtSettings mapAdminToJwtSettings(AdminSettings adminSettings) {
        Objects.requireNonNull(adminSettings, "adminSettings for JWT is null");
        return JacksonUtil.treeToValue(adminSettings.getJsonValue(), JwtSettings.class);
    }

    private AdminSettings mapJwtToAdminSettings(JwtSettings jwtSettings) {
        Objects.requireNonNull(jwtSettings, "jwtSettings is null");
        AdminSettings adminJwtSettings = new AdminSettings();
        adminJwtSettings.setTenantId(TenantId.SYS_TENANT_ID);
        adminJwtSettings.setKey(ADMIN_SETTINGS_JWT_KEY);
        adminJwtSettings.setJsonValue(JacksonUtil.valueToTree(jwtSettings));
        return adminJwtSettings;
    }

    /**
     * 判断是否仍使用默认签发密钥。
     */
    public static boolean isSigningKeyDefault(JwtSettings settings) {
        return TOKEN_SIGNING_KEY_DEFAULT.equals(settings.getTokenSigningKey());
    }

    /**
     * 校验 Base64 签发密钥解码后位数是否达到 {@link JwtTokenFactory#KEY_LENGTH}。
     */
    public static boolean validateKeyLength(String key) {
        return Base64.getDecoder().decode(key).length * Byte.SIZE >= KEY_LENGTH;
    }

}
