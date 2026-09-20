package com.jnks.iot.server.dao.queue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.QueueStatsId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.queue.QueueStats;
import com.jnks.iot.server.dao.entity.AbstractEntityService;
import com.jnks.iot.server.dao.eventsourcing.DeleteEntityEvent;
import com.jnks.iot.server.dao.eventsourcing.SaveEntityEvent;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.service.Validator;

import java.util.List;
import java.util.Optional;

import static com.jnks.iot.server.dao.service.Validator.validateId;
import static com.jnks.iot.server.dao.service.Validator.validateIds;

@Service("QueueStatsDaoService")
@Slf4j
@RequiredArgsConstructor
public class BaseQueueStatsService extends AbstractEntityService implements QueueStatsService {

    public static final String INCORRECT_TENANT_ID = "Incorrect tenantId ";

    private final QueueStatsDao queueStatsDao;

    private final DataValidator<QueueStats> queueStatsValidator;

    @Override
    public QueueStats save(TenantId tenantId, QueueStats queueStats) {
        log.trace("Executing save [{}]", queueStats);
        queueStatsValidator.validate(queueStats, QueueStats::getTenantId);
        QueueStats savedQueueStats = queueStatsDao.save(tenantId, queueStats);
        eventPublisher.publishEvent(SaveEntityEvent.builder().tenantId(savedQueueStats.getTenantId()).entityId(savedQueueStats.getId())
                .entity(savedQueueStats).created(queueStats.getId() == null).build());
        return savedQueueStats;
    }

    @Override
    public QueueStats findQueueStatsById(TenantId tenantId, QueueStatsId queueStatsId) {
        log.trace("Executing findQueueStatsById [{}]", queueStatsId);
        validateId(queueStatsId, id -> "Incorrect queueStatsId " + id);
        return queueStatsDao.findById(tenantId, queueStatsId.getId());
    }

    @Override
    public List<QueueStats> findQueueStatsByIds(TenantId tenantId, List<QueueStatsId> queueStatsIds) {
        log.trace("Executing findQueueStatsByIds, tenantId [{}], queueStatsIds [{}]", tenantId, queueStatsIds);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        validateIds(queueStatsIds, ids -> "Incorrect queueStatsIds " + ids);
        return queueStatsDao.findByIds(tenantId, queueStatsIds);
    }

    @Override
    public QueueStats findByTenantIdAndNameAndServiceId(TenantId tenantId, String queueName, String serviceId) {
        log.trace("Executing findByTenantIdAndNameAndServiceId, tenantId: [{}], queueName: [{}], serviceId: [{}]", tenantId, queueName, serviceId);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        return queueStatsDao.findByTenantIdQueueNameAndServiceId(tenantId, queueName, serviceId);
    }

    @Override
    public PageData<QueueStats> findByTenantId(TenantId tenantId, PageLink pageLink) {
        log.trace("Executing findByTenantId, tenantId: [{}]", tenantId);
        Validator.validatePageLink(pageLink);
        return queueStatsDao.findAllByTenantId(tenantId, pageLink);
    }

    @Override
    public void deleteByTenantId(TenantId tenantId) {
        log.trace("Executing deleteByTenantId, tenantId [{}]", tenantId);
        validateId(tenantId, id -> INCORRECT_TENANT_ID + id);
        queueStatsDao.deleteByTenantId(tenantId);
    }

    @Override
    public void deleteEntity(TenantId tenantId, EntityId id, boolean force) {
        queueStatsDao.removeById(tenantId, id.getId());
        eventPublisher.publishEvent(DeleteEntityEvent.builder().tenantId(tenantId).entityId(id).build());
    }

    @Override
    public Optional<HasId<?>> findEntity(TenantId tenantId, EntityId entityId) {
        return Optional.ofNullable(findQueueStatsById(tenantId, new QueueStatsId(entityId.getId())));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.QUEUE_STATS;
    }

}
