package com.jnks.iot.server.service.security.permission;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.security.Authority;
import com.jnks.iot.server.service.security.model.SecurityUser;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * {@link AccessControlService} 默认实现。
 * <p>
 * 将三种 {@link Authority} 映射到对应 {@link Permissions} Bean，委托 {@link PermissionChecker} 判定。
 *
 * @see AccessControlService
 */
@Service
@Slf4j
public class DefaultAccessControlService implements AccessControlService {

    private static final String INCORRECT_TENANT_ID = "Incorrect tenantId ";
    private static final String YOU_DON_T_HAVE_PERMISSION_TO_PERFORM_THIS_OPERATION = "You don't have permission to perform this operation!";

    private final Map<Authority, Permissions> authorityPermissions = new HashMap<>();

    /**
     * 按角色注册系统管理员、租户管理员、客户用户权限表。
     */
    public DefaultAccessControlService(
            @Qualifier("sysAdminPermissions") Permissions sysAdminPermissions,
            @Qualifier("tenantAdminPermissions") Permissions tenantAdminPermissions,
            @Qualifier("customerUserPermissions") Permissions customerUserPermissions) {
        authorityPermissions.put(Authority.SYS_ADMIN, sysAdminPermissions);
        authorityPermissions.put(Authority.TENANT_ADMIN, tenantAdminPermissions);
        authorityPermissions.put(Authority.CUSTOMER_USER, customerUserPermissions);
    }

    /**
     * 校验资源级操作权限。
     */
    @Override
    public void checkPermission(SecurityUser user, Resource resource, Operation operation) throws JnksIotException {
        PermissionChecker permissionChecker = getPermissionChecker(user.getAuthority(), resource);
        if (!permissionChecker.hasPermission(user, operation)) {
            permissionDenied();
        }
    }

    /**
     * 校验实体级操作权限。
     */
    @Override
    @SuppressWarnings("unchecked")
    public <I extends EntityId, T extends HasTenantId> void checkPermission(SecurityUser user, Resource resource,
                                                                                            Operation operation, I entityId, T entity) throws JnksIotException {
        PermissionChecker permissionChecker = getPermissionChecker(user.getAuthority(), resource);
        if (!permissionChecker.hasPermission(user, operation, entityId, entity)) {
            permissionDenied();
        }
    }

    private PermissionChecker getPermissionChecker(Authority authority, Resource resource) throws JnksIotException {
        Permissions permissions = authorityPermissions.get(authority);
        if (permissions == null) {
            permissionDenied();
        }
        Optional<PermissionChecker> permissionChecker = permissions.getPermissionChecker(resource);
        if (!permissionChecker.isPresent()) {
            permissionDenied();
        }
        return permissionChecker.get();
    }

    private void permissionDenied() throws JnksIotException {
        throw new JnksIotException(YOU_DON_T_HAVE_PERMISSION_TO_PERFORM_THIS_OPERATION,
                JnksIotErrorCode.PERMISSION_DENIED);
    }

}
