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

import com.fasterxml.jackson.databind.JsonNode;
import org.thingsboard.server.common.data.id.DashboardId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.UserId;
import org.thingsboard.server.common.data.settings.UserDashboardAction;
import org.thingsboard.server.common.data.settings.UserDashboardsInfo;
import org.thingsboard.server.common.data.settings.UserSettings;
import org.thingsboard.server.common.data.settings.UserSettingsType;

import java.util.List;

/**
 * 用户个性化设置契约：通用 JSON 设置以及最近访问/收藏仪表板。
 */
public interface TbUserSettingsService {

    /** 合并更新指定类型的用户设置。 */
    void updateUserSettings(TenantId tenantId, UserId userId, UserSettingsType type, JsonNode settings);

    /** 整份保存用户设置。 */
    UserSettings saveUserSettings(TenantId tenantId, UserSettings userSettings);

    /** 按类型查询用户设置。 */
    UserSettings findUserSettings(TenantId tenantId, UserId userId, UserSettingsType type);

    /** 按 JSON Path 删除用户设置中的字段。 */
    void deleteUserSettings(TenantId tenantId, UserId userId, UserSettingsType type, List<String> jsonPaths);

    /** 查询用户最近访问与收藏的仪表板信息。 */
    UserDashboardsInfo findUserDashboardsInfo(TenantId tenantId, UserId id);

    /** 记录访问/收藏/取消收藏仪表板动作并回写设置。 */
    UserDashboardsInfo reportUserDashboardAction(TenantId tenantId, UserId id, DashboardId dashboardId, UserDashboardAction action);
}
