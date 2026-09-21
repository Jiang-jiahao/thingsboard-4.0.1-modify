package com.jnks.iot.server.service.entitiy.user;

import jakarta.servlet.http.HttpServletRequest;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.UserActivationLink;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;

/**
 * 用户业务层契约：保存（可选发激活邮件）、删除与获取激活链接。
 * <p>
 * 由 UserController 调用；实现类委托 User DAO，写审计日志，新建时可发邮件。
 */
public interface JnksIotUserService {

    /** 保存用户；新建且 {@code sendActivationMail} 为 true 时发送激活邮件。 */
    User save(TenantId tenantId, CustomerId customerId, User jnksIotUser, boolean sendActivationMail, HttpServletRequest request, User user) throws JnksIotException;

    /** 删除用户。 */
    void delete(TenantId tenantId, CustomerId customerId, User user, User responsibleUser) throws JnksIotException;

    /** 生成尚未激活用户的激活链接。 */
    UserActivationLink getActivationLink(TenantId tenantId, CustomerId customerId, UserId userId, HttpServletRequest request) throws JnksIotException;

}
