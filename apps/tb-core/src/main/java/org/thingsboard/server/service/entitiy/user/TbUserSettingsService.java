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
