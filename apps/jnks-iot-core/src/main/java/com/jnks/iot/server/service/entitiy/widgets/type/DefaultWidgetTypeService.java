package com.jnks.iot.server.service.entitiy.widgets.type;

import lombok.AllArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.widget.WidgetType;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.dao.widget.WidgetTypeService;
import com.jnks.iot.server.service.entitiy.AbstractJnksIotEntityService;
import com.jnks.iot.server.service.resource.JnksIotResourceService;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * {@link JnksIotWidgetTypeService} 的默认实现。
 * <p>
 * 由 WidgetTypeController 调用，委托 {@link WidgetTypeService} 落库；可导入关联资源，
 * 写审计日志，保存时 autoCommit。
 *
 * @see JnksIotWidgetTypeService
 */
@Service
@AllArgsConstructor
public class DefaultWidgetTypeService extends AbstractJnksIotEntityService implements JnksIotWidgetTypeService {

    private final WidgetTypeService widgetTypeService;
    private final JnksIotResourceService jnksIotResourceService;

    /** 保存部件类型（不按 FQN 覆盖），委托带标志的重载。 */
    @Override
    public WidgetTypeDetails save(WidgetTypeDetails entity, SecurityUser user) throws Exception {
        return this.save(entity, false, user);
    }

    /** 保存部件类型；可按 FQN 覆盖已有记录，并写审计。 */
    @Override
    public WidgetTypeDetails save(WidgetTypeDetails widgetTypeDetails, boolean updateExistingByFqn, SecurityUser user) throws Exception {
        TenantId tenantId = widgetTypeDetails.getTenantId();
        if (widgetTypeDetails.getId() == null && StringUtils.isNotEmpty(widgetTypeDetails.getFqn()) && updateExistingByFqn) {
            WidgetType widgetType = widgetTypeService.findWidgetTypeByTenantIdAndFqn(tenantId, widgetTypeDetails.getFqn());
            if (widgetType != null) {
                widgetTypeDetails.setId(widgetType.getId());
            }
        }
        if (CollectionUtils.isNotEmpty(widgetTypeDetails.getResources())) {
            jnksIotResourceService.importResources(widgetTypeDetails.getResources(), user);
        }

        ActionType actionType = widgetTypeDetails.getId() == null ? ActionType.ADDED : ActionType.UPDATED;
        try {
            WidgetTypeDetails savedWidgetTypeDetails = checkNotNull(widgetTypeService.saveWidgetType(widgetTypeDetails));
            autoCommit(user, savedWidgetTypeDetails.getId());
            logEntityActionService.logEntityAction(tenantId, savedWidgetTypeDetails.getId(), savedWidgetTypeDetails,
                    null, actionType, user);
            return savedWidgetTypeDetails;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.WIDGET_TYPE), widgetTypeDetails, actionType, user, e);
            throw e;
        }
    }

    /** 删除部件类型并写 DELETED 审计。 */
    @Override
    public void delete(WidgetTypeDetails widgetTypeDetails, User user) {
        ActionType actionType = ActionType.DELETED;
        TenantId tenantId = widgetTypeDetails.getTenantId();
        try {
            widgetTypeService.deleteWidgetType(widgetTypeDetails.getTenantId(), widgetTypeDetails.getId());
            logEntityActionService.logEntityAction(tenantId, widgetTypeDetails.getId(), widgetTypeDetails, null, actionType, user);
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.WIDGET_TYPE), actionType, user, e, widgetTypeDetails.getId());
            throw e;
        }
    }

}
