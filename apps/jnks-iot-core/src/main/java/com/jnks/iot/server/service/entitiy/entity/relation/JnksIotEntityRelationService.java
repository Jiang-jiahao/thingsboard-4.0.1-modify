package com.jnks.iot.server.service.entitiy.entity.relation;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.relation.EntityRelation;

/**
 * 实体关系业务层契约：保存、删除单条关系或某实体的 COMMON 关系。
 * <p>
 * 由 EntityRelationController 调用；实现类委托 Relation DAO，并对关系两端写审计日志。
 */
public interface JnksIotEntityRelationService {

    /** 保存实体关系。 */
    EntityRelation save(TenantId tenantId, CustomerId customerId, EntityRelation entity, User user) throws JnksIotException;

    /** 删除单条实体关系。 */
    EntityRelation delete(TenantId tenantId, CustomerId customerId, EntityRelation entity, User user) throws JnksIotException;

    /** 删除某实体上所有 COMMON 类型关系。 */
    void deleteCommonRelations(TenantId tenantId, CustomerId customerId, EntityId entityId, User user) throws JnksIotException;

}
