package com.jnks.iot.server.dao.sql.alarm;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.alarm.AlarmComment;
import com.jnks.iot.server.common.data.alarm.AlarmCommentInfo;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.TenantEntityDao;
import com.jnks.iot.server.dao.alarm.AlarmCommentDao;
import com.jnks.iot.server.dao.model.sql.AlarmCommentEntity;
import com.jnks.iot.server.dao.sql.JpaPartitionedAbstractDao;
import com.jnks.iot.server.dao.sqlts.insert.sql.SqlPartitioningRepository;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.jnks.iot.server.dao.model.ModelConstants.ALARM_COMMENT_TABLE_NAME;

@Slf4j
@Component
@SqlDao
@RequiredArgsConstructor
public class JpaAlarmCommentDao extends JpaPartitionedAbstractDao<AlarmCommentEntity, AlarmComment> implements AlarmCommentDao, TenantEntityDao<AlarmComment> {
    private final SqlPartitioningRepository partitioningRepository;
    @Value("${sql.alarm_comments.partition_size:168}")
    private int partitionSizeInHours;

    @Autowired
    private AlarmCommentRepository alarmCommentRepository;

    @Override
    public PageData<AlarmCommentInfo> findAlarmComments(TenantId tenantId, AlarmId id, PageLink pageLink) {
        log.trace("Try to find alarm comments by alarm id using [{}]", id);
        return DaoUtil.toPageData(
                alarmCommentRepository.findAllByAlarmId(id.getId(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    public AlarmComment findAlarmCommentById(TenantId tenantId, UUID key) {
        log.trace("Try to find alarm comment by id using [{}]", key);
        return DaoUtil.getData(alarmCommentRepository.findById(key));
    }

    @Override
    public ListenableFuture<AlarmComment> findAlarmCommentByIdAsync(TenantId tenantId, UUID key) {
        log.trace("Try to find alarm comment by id using [{}]", key);
        return findByIdAsync(tenantId, key);
    }

    @Override
    public void createPartition(AlarmCommentEntity entity) {
        partitioningRepository.createPartitionIfNotExists(ALARM_COMMENT_TABLE_NAME, entity.getCreatedTime(), TimeUnit.HOURS.toMillis(partitionSizeInHours));
    }

    @Override
    public PageData<AlarmComment> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(alarmCommentRepository.findByTenantId(tenantId.getId(), DaoUtil.toPageable(pageLink)));
    }

    @Override
    protected Class<AlarmCommentEntity> getEntityClass() {
        return AlarmCommentEntity.class;
    }

    @Override
    protected JpaRepository<AlarmCommentEntity, UUID> getRepository() {
        return alarmCommentRepository;
    }

}
