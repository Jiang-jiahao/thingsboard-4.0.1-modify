package com.jnks.iot.server.dao.sql.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.QueueStatsFields;
import com.jnks.iot.server.common.data.id.QueueStatsId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.queue.QueueStats;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.model.sql.QueueStatsEntity;
import com.jnks.iot.server.dao.queue.QueueStatsDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

import static com.jnks.iot.server.dao.DaoUtil.toUUIDs;

@Slf4j
@Component
@SqlDao
public class JpaQueueStatsDao extends JpaAbstractDao<QueueStatsEntity, QueueStats> implements QueueStatsDao {

    @Autowired
    private QueueStatsRepository queueStatsRepository;

    @Override
    protected Class<QueueStatsEntity> getEntityClass() {
        return QueueStatsEntity.class;
    }

    @Override
    protected JpaRepository<QueueStatsEntity, UUID> getRepository() {
        return queueStatsRepository;
    }

    @Override
    public QueueStats findByTenantIdQueueNameAndServiceId(TenantId tenantId, String queueName, String serviceId) {
        return DaoUtil.getData(queueStatsRepository.findByTenantIdAndQueueNameAndServiceId(tenantId.getId(), queueName, serviceId));
    }

    @Override
    public PageData<QueueStats> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(queueStatsRepository.findByTenantId(tenantId.getId(), pageLink.getTextSearch(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public void deleteByTenantId(TenantId tenantId) {
        queueStatsRepository.deleteByTenantId(tenantId.getId());
    }

    @Override
    public List<QueueStats> findByIds(TenantId tenantId, List<QueueStatsId> queueStatsIds) {
        return DaoUtil.convertDataList(queueStatsRepository.findByTenantIdAndIdIn(tenantId.getId(), toUUIDs(queueStatsIds)));
    }

    @Override
    public List<QueueStatsFields> findNextBatch(UUID id, int batchSize) {
        return queueStatsRepository.findNextBatch(id, Limit.of(batchSize));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.QUEUE_STATS;
    }

}
