package com.jnks.iot.server.service.housekeeper.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.housekeeper.EntitiesDeletionHousekeeperTask;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.common.data.housekeeper.TenantEntitiesDeletionHousekeeperTask;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.entity.EntityDaoRegistry;

import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class TenantEntitiesDeletionTaskProcessor extends HousekeeperTaskProcessor<TenantEntitiesDeletionHousekeeperTask> {

    private final EntityDaoRegistry entityDaoRegistry;

    @Override
    public void process(TenantEntitiesDeletionHousekeeperTask task) throws Exception {
        EntityType entityType = task.getEntityType();
        TenantId tenantId = task.getTenantId();
        Dao<?> entityDao = entityDaoRegistry.getDao(entityType);

        UUID last = null;
        while (true) {
            List<UUID> entities = entityDao.findIdsByTenantIdAndIdOffset(tenantId, last, 128);
            if (entities.isEmpty()) {
                break;
            }

            housekeeperClient.submitTask(new EntitiesDeletionHousekeeperTask(tenantId, entityType, entities));
            last = entities.get(entities.size() - 1);
            log.debug("[{}] Submitted task for deleting {} {}s", tenantId, entities.size(), entityType.getNormalName().toLowerCase());
        }
    }

    @Override
    public HousekeeperTaskType getTaskType() {
        return HousekeeperTaskType.DELETE_TENANT_ENTITIES;
    }

}
