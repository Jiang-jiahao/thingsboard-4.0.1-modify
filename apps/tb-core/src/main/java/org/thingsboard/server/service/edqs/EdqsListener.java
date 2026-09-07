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
