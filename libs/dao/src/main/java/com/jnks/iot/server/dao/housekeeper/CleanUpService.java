package com.jnks.iot.server.dao.housekeeper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionalEventListener;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTask;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.housekeeper.HousekeeperClient;
import com.jnks.iot.server.dao.eventsourcing.ActionCause;
import com.jnks.iot.server.dao.eventsourcing.DeleteEntityEvent;
import com.jnks.iot.server.dao.relation.RelationService;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

@Component
@RequiredArgsConstructor
@Slf4j
public class CleanUpService {

    private final Optional<HousekeeperClient> housekeeperClient;
    private final RelationService relationService;

    private final Set<EntityType> skippedEntities = EnumSet.of(
            EntityType.ALARM, EntityType.QUEUE, EntityType.JNKS_IOT_RESOURCE, EntityType.OTA_PACKAGE,
            EntityType.NOTIFICATION_REQUEST, EntityType.NOTIFICATION_TEMPLATE,
            EntityType.NOTIFICATION_TARGET, EntityType.NOTIFICATION_RULE
    );

    @TransactionalEventListener(fallbackExecution = true) // after transaction commit
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void handleEntityDeletionEvent(DeleteEntityEvent<?> event) {
        TenantId tenantId = event.getTenantId();
        EntityId entityId = event.getEntityId();
        EntityType entityType = entityId.getEntityType();
        try {
            log.trace("[{}][{}][{}] Handling entity deletion event", tenantId, entityType, entityId.getId());
            if (!skippedEntities.contains(entityType)) {
                cleanUpRelatedData(tenantId, entityId);
            }
            if (entityType == EntityType.USER && event.getCause() != ActionCause.TENANT_DELETION) {
                submitTask(HousekeeperTask.unassignAlarms((User) event.getEntity()));
            }
        } catch (Throwable e) {
            log.error("[{}][{}][{}] Failed to handle entity deletion event", tenantId, entityType, entityId.getId(), e);
        }
    }

    public void cleanUpRelatedData(TenantId tenantId, EntityId entityId) {
        log.debug("[{}][{}][{}] Cleaning up related data", tenantId, entityId.getEntityType(), entityId.getId());
        relationService.deleteEntityRelations(tenantId, entityId);
        submitTask(HousekeeperTask.deleteAttributes(tenantId, entityId));
        submitTask(HousekeeperTask.deleteTelemetry(tenantId, entityId));
        submitTask(HousekeeperTask.deleteEvents(tenantId, entityId));
        submitTask(HousekeeperTask.deleteAlarms(tenantId, entityId));
        submitTask(HousekeeperTask.deleteCalculatedFields(tenantId, entityId));
    }

    public void removeTenantEntities(TenantId tenantId, EntityType... entityTypes) {
        for (EntityType entityType : entityTypes) {
            submitTask(HousekeeperTask.deleteTenantEntities(tenantId, entityType));
        }
    }

    private void submitTask(HousekeeperTask task) {
        housekeeperClient.ifPresent(housekeeperClient -> {
            housekeeperClient.submitTask(task);
        });
    }

}
