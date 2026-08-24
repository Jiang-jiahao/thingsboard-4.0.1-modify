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
package org.thingsboard.server.service.entitiy.widgets.bundle;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.id.WidgetTypeId;
import org.thingsboard.server.common.data.id.WidgetsBundleId;
import org.thingsboard.server.common.data.widget.WidgetsBundle;
import org.thingsboard.server.dao.widget.WidgetTypeService;
import org.thingsboard.server.dao.widget.WidgetsBundleService;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.entitiy.AbstractTbEntityService;
import org.thingsboard.server.service.security.model.SecurityUser;

import java.util.List;

/**
 * {@link TbWidgetsBundleService} 的默认实现。
 * <p>
 * 由 WidgetsBundleController 调用，委托 {@link WidgetsBundleService} 落库；写审计日志，保存/更新内容时 autoCommit。
 *
 * @see TbWidgetsBundleService
 */
@Service
@TbCoreComponent
@AllArgsConstructor
@Slf4j
public class DefaultWidgetsBundleService extends AbstractTbEntityService implements TbWidgetsBundleService {

    private final WidgetsBundleService widgetsBundleService;
    private final WidgetTypeService widgetTypeService;

    /** 保存部件包，写审计并尝试 autoCommit。 */
    @Override
    public WidgetsBundle save(WidgetsBundle widgetsBundle, SecurityUser user) throws Exception {
        ActionType actionType = widgetsBundle.getId() == null ? ActionType.ADDED : ActionType.UPDATED;
        TenantId tenantId = widgetsBundle.getTenantId();
        try {
            WidgetsBundle savedWidgetsBundle = checkNotNull(widgetsBundleService.saveWidgetsBundle(widgetsBundle));
            autoCommit(user, savedWidgetsBundle.getId());
            logEntityActionService.logEntityAction(tenantId, savedWidgetsBundle.getId(), savedWidgetsBundle,
                    null, actionType, user);
            return savedWidgetsBundle;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.WIDGETS_BUNDLE), widgetsBundle, actionType, user, e);
            throw e;
        }
    }

    /** 删除部件包并写 DELETED 审计。 */
    @Override
    public void delete(WidgetsBundle widgetsBundle, User user) {
        ActionType actionType = ActionType.DELETED;
        TenantId tenantId = widgetsBundle.getTenantId();
        try {
            widgetsBundleService.deleteWidgetsBundle(widgetsBundle.getTenantId(), widgetsBundle.getId());
            logEntityActionService.logEntityAction(tenantId, widgetsBundle.getId(), widgetsBundle, null, actionType, user);
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.WIDGETS_BUNDLE), actionType, user, e, widgetsBundle.getId());
            throw e;
        }
    }

    /** 按部件类型 ID 更新包内容并 autoCommit。 */
    @Override
    public void updateWidgetsBundleWidgetTypes(WidgetsBundleId widgetsBundleId, List<WidgetTypeId> widgetTypeIds, User user) throws Exception {
        widgetTypeService.updateWidgetsBundleWidgetTypes(user.getTenantId(), widgetsBundleId, widgetTypeIds);
        autoCommit(user, widgetsBundleId);
    }

    /** 按部件 FQN 更新包内容并 autoCommit。 */
    @Override
    public void updateWidgetsBundleWidgetFqns(WidgetsBundleId widgetsBundleId, List<String> widgetFqns, User user) throws Exception {
        widgetTypeService.updateWidgetsBundleWidgetFqns(user.getTenantId(), widgetsBundleId, widgetFqns);
        autoCommit(user, widgetsBundleId);
    }

}
