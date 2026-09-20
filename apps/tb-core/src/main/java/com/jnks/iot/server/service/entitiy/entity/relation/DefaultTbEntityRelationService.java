package com.jnks.iot.server.service.entitiy.entity.relation;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.dao.relation.RelationService;
import com.jnks.iot.server.service.entitiy.AbstractTbEntityService;

/**
 * {@link TbEntityRelationService} 的默认实现。
 * <p>
 * 由 EntityRelationController 调用，委托 {@link RelationService} 落库；
 * 对关系两端各记一次审计（经 {@code logEntityRelationAction}）。
 *
 * @see TbEntityRelationService
 */
@Service
@AllArgsConstructor
@Slf4j
public class DefaultTbEntityRelationService extends AbstractTbEntityService implements TbEntityRelationService {

    private final RelationService relationService;

    /** 保存实体关系并对两端写审计。 */
    @Override
    public EntityRelation save(TenantId tenantId, CustomerId customerId, EntityRelation relation, User user) throws JnksIotException {
        ActionType actionType = ActionType.RELATION_ADD_OR_UPDATE;
        try {
            var savedRelation = relationService.saveRelation(tenantId, relation);
            logEntityActionService.logEntityRelationAction(tenantId, customerId,
                    savedRelation, user, actionType, null, savedRelation);
            return savedRelation;
        } catch (Exception e) {
            logEntityActionService.logEntityRelationAction(tenantId, customerId,
                    relation, user, actionType, e, relation);
            throw e;
        }
    }

    /** 删除单条实体关系并对两端写审计。 */
    @Override
    public EntityRelation delete(TenantId tenantId, CustomerId customerId, EntityRelation relation, User user) throws JnksIotException {
        ActionType actionType = ActionType.RELATION_DELETED;
        try {
            var found = relationService.deleteRelation(tenantId, relation.getFrom(), relation.getTo(), relation.getType(), relation.getTypeGroup());
            if (found == null) {
                throw new JnksIotException("Requested item wasn't found!", JnksIotErrorCode.ITEM_NOT_FOUND);
            }
            logEntityActionService.logEntityRelationAction(tenantId, customerId, found, user, actionType, null, found);
            return found;
        } catch (Exception e) {
            logEntityActionService.logEntityRelationAction(tenantId, customerId,
                    relation, user, actionType, e, relation);
            throw e;
        }
    }

    /** 删除某实体上所有 COMMON 关系并写审计。 */
    @Override
    public void deleteCommonRelations(TenantId tenantId, CustomerId customerId, EntityId entityId, User user) throws JnksIotException {
        try {
            relationService.deleteEntityCommonRelations(tenantId, entityId);
            logEntityActionService.logEntityAction(tenantId, entityId, null, customerId, ActionType.RELATIONS_DELETED, user);
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, entityId, null, customerId,
                    ActionType.RELATIONS_DELETED, user, e);
            throw e;
        }
    }
}
