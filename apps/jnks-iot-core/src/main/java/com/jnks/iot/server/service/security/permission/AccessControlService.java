package com.jnks.iot.server.service.security.permission;

import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * REST / WebSocket 访问控制入口。
 * <p>
 * 按用户角色（系统管理员 / 租户管理员 / 客户用户）选择权限表，校验资源级或实体级操作。
 *
 * @see DefaultAccessControlService
 */
public interface AccessControlService {

    /**
     * 校验用户是否具备某资源上的操作权限（不绑定具体实体）。
     */
    void checkPermission(SecurityUser user, Resource resource, Operation operation) throws JnksIotException;

    /**
     * 校验用户对指定实体的操作权限。
     */
    <I extends EntityId, T extends HasTenantId> void checkPermission(SecurityUser user, Resource resource, Operation operation, I entityId, T entity) throws JnksIotException;

}
