package com.jnks.iot.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.NotificationTargetId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.targets.NotificationTarget;
import com.jnks.iot.server.common.data.notification.targets.NotificationTargetType;
import com.jnks.iot.server.common.data.notification.targets.platform.CustomerUsersFilter;
import com.jnks.iot.server.common.data.notification.targets.platform.PlatformUsersNotificationTargetConfig;
import com.jnks.iot.server.common.data.notification.targets.platform.TenantAdministratorsFilter;
import com.jnks.iot.server.common.data.notification.targets.platform.UserListFilter;
import com.jnks.iot.server.common.data.notification.targets.platform.UsersFilter;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.dao.notification.NotificationTargetService;
import com.jnks.iot.server.dao.service.ConstraintValidator;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

import java.util.List;

/**
 * 针对 {@link NotificationTarget} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 客户用户过滤器映射内部 customerId；用户列表替换为当前导入用户（VC 不支持 User 实体）；
 * 拒绝带租户/租户 Profile 过滤的租户管理员目标以及系统管理员目标。
 */
@Service
@RequiredArgsConstructor
public class NotificationTargetImportService extends BaseEntityImportService<NotificationTargetId, NotificationTarget, EntityExportData<NotificationTarget>> {

    private final NotificationTargetService notificationTargetService;

    @Override
    protected void setOwner(TenantId tenantId, NotificationTarget notificationTarget, IdProvider idProvider) {
        notificationTarget.setTenantId(tenantId);
    }

    /**
     * 映射客户过滤器；用户列表改为当前用户；拒绝跨租户或系统管理员目标。
     */
    @Override
    protected NotificationTarget prepare(EntitiesImportCtx ctx, NotificationTarget notificationTarget, NotificationTarget oldNotificationTarget, EntityExportData<NotificationTarget> exportData, IdProvider idProvider) {
        if (notificationTarget.getConfiguration().getType() == NotificationTargetType.PLATFORM_USERS) {
            UsersFilter usersFilter = ((PlatformUsersNotificationTargetConfig) notificationTarget.getConfiguration()).getUsersFilter();
            switch (usersFilter.getType()) {
                case CUSTOMER_USERS:
                    CustomerUsersFilter customerUsersFilter = (CustomerUsersFilter) usersFilter;
                    customerUsersFilter.setCustomerId(idProvider.getInternalId(new CustomerId(customerUsersFilter.getCustomerId())).getId());
                    break;
                case USER_LIST:
                    UserListFilter userListFilter = (UserListFilter) usersFilter;
                    userListFilter.setUsersIds(List.of(ctx.getUser().getUuidId())); // user entities are not supported by VC; replacing with current user id
                    break;
                case TENANT_ADMINISTRATORS:
                    if (CollectionUtils.isNotEmpty(((TenantAdministratorsFilter) usersFilter).getTenantsIds()) ||
                            CollectionUtils.isNotEmpty(((TenantAdministratorsFilter) usersFilter).getTenantProfilesIds())) {
                        throw new IllegalArgumentException("Permission denied");
                    }
                    break;
                case SYSTEM_ADMINISTRATORS:
                    throw new AccessDeniedException("Permission denied");
            }
        }
        return notificationTarget;
    }

    /** 校验字段后保存通知目标。 */
    @Override
    protected NotificationTarget saveOrUpdate(EntitiesImportCtx ctx, NotificationTarget notificationTarget, EntityExportData<NotificationTarget> exportData, IdProvider idProvider) {
        ConstraintValidator.validateFields(notificationTarget);
        return notificationTargetService.saveNotificationTarget(ctx.getTenantId(), notificationTarget);
    }

    @Override
    protected void onEntitySaved(User user, NotificationTarget savedEntity, NotificationTarget oldEntity) throws JnksIotException {
        entityActionService.logEntityAction(user, savedEntity.getId(), savedEntity, null,
                oldEntity == null ? ActionType.ADDED : ActionType.UPDATED, null);
    }

    @Override
    protected NotificationTarget deepCopy(NotificationTarget notificationTarget) {
        return new NotificationTarget(notificationTarget);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.NOTIFICATION_TARGET;
    }

}
