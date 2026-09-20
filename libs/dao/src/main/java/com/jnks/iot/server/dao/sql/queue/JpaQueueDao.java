package com.jnks.iot.server.dao.sql.queue;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.queue.Queue;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.model.sql.QueueEntity;
import com.jnks.iot.server.dao.queue.QueueDao;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@SqlDao
public class JpaQueueDao extends JpaAbstractDao<QueueEntity, Queue> implements QueueDao, TenantEntityDao<Queue> {

    @Autowired
    private QueueRepository queueRepository;

    @Override
    protected Class<QueueEntity> getEntityClass() {
        return QueueEntity.class;
    }

    @Override
    protected JpaRepository<QueueEntity, UUID> getRepository() {
        return queueRepository;
    }

    @Override
    public Queue findQueueByTenantIdAndTopic(TenantId tenantId, String topic) {
        return DaoUtil.getData(queueRepository.findByTenantIdAndTopic(tenantId.getId(), topic));
    }

    @Override
    public Queue findQueueByTenantIdAndName(TenantId tenantId, String name) {
        return DaoUtil.getData(queueRepository.findByTenantIdAndName(tenantId.getId(), name));
    }

    @Override
    public List<Queue> findAllByTenantId(TenantId tenantId) {
        List<QueueEntity> entities = queueRepository.findByTenantId(tenantId.getId());
        return DaoUtil.convertDataList(entities);
    }

    @Override
    public List<Queue> findAllMainQueues() {
        List<QueueEntity> entities = Lists.newArrayList(queueRepository.findAllByName(DataConstants.MAIN_QUEUE_NAME));
        return DaoUtil.convertDataList(entities);
    }

    @Override
    public List<Queue> findAllQueues() {
        List<QueueEntity> entities = Lists.newArrayList(queueRepository.findAll());
        return DaoUtil.convertDataList(entities);
    }

    @Override
    public PageData<Queue> findQueuesByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(queueRepository
                .findByTenantId(tenantId.getId(), pageLink.getTextSearch(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public PageData<Queue> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return findQueuesByTenantId(tenantId, pageLink);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.QUEUE;
    }

}
