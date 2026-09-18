package org.thingsboard.server.service.entitiy;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.service.security.model.SecurityUser;

/**
 * 仅含保存/删除的实体业务契约，供客户、仪表板、部件包等简单 CRUD 服务复用。
 *
 * @param <T> 实体类型
 */
public interface SimpleTbEntityService<T> {

    /** 无用户上下文保存，默认委托 {@link #save(Object, SecurityUser)}。 */
    default T save(T entity) throws Exception {
        return save(entity, null);
    }

    /** 保存实体；{@code user} 用于审计与版本控制自动提交。 */
    T save(T entity, SecurityUser user) throws Exception;

    /** 删除实体并记录操作用户。 */
    void delete(T entity, User user);

}
