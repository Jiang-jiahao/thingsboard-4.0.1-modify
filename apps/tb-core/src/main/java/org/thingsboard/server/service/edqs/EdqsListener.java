package org.thingsboard.server.service.edqs;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.event.TransactionalEventListener;
import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.msg.edqs.EdqsService;
import org.thingsboard.server.dao.eventsourcing.DeleteEntityEvent;
import org.thingsboard.server.dao.eventsourcing.RelationActionEvent;
import org.thingsboard.server.dao.eventsourcing.SaveEntityEvent;

/**
 * EDQS 实体变更监听器。
 * <p>
 * 监听 DAO 层事务提交后的保存/删除/关系事件，转发给 {@link EdqsService} 写入 EDQS。
 * 仅在 {@code queue.edqs.sync.enabled=true} 时生效。
 */
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(value = "queue.edqs.sync.enabled", havingValue = "true")
public class EdqsListener {

    private final EdqsService edqsService;

    /** 实体保存/更新后，同步推送 UPDATED 事件 */
    @TransactionalEventListener(fallbackExecution = true)
    public void onUpdate(SaveEntityEvent<?> event) {
        if (event.getEntityId() == null || event.getEntity() == null) {
            return;
        }
        edqsService.onUpdate(event.getTenantId(), event.getEntityId(), event.getEntity());
    }

    /** 实体删除后，同步推送 DELETED 事件 */
    @TransactionalEventListener(fallbackExecution = true)
    public void onDelete(DeleteEntityEvent<?> event) {
        if (event.getEntityId() == null) {
            return;
        }
        edqsService.onDelete(event.getTenantId(), event.getEntityId());
    }

    /** 关系新增/更新/删除后，按动作类型推送对应 EDQS 事件 */
    @TransactionalEventListener(fallbackExecution = true)
    public void handleEvent(RelationActionEvent relationEvent) {
        if (relationEvent.getActionType() == ActionType.RELATION_ADD_OR_UPDATE) {
            edqsService.onUpdate(relationEvent.getTenantId(), ObjectType.RELATION, relationEvent.getRelation());
        } else if (relationEvent.getActionType() == ActionType.RELATION_DELETED) {
            edqsService.onDelete(relationEvent.getTenantId(), ObjectType.RELATION, relationEvent.getRelation());
        }
    }

}
