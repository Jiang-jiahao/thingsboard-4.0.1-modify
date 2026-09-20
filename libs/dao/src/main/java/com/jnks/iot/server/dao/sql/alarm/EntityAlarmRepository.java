package com.jnks.iot.server.dao.sql.alarm;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import com.jnks.iot.server.dao.model.sql.EntityAlarmCompositeKey;
import com.jnks.iot.server.dao.model.sql.EntityAlarmEntity;

import java.util.List;
import java.util.UUID;

public interface EntityAlarmRepository extends JpaRepository<EntityAlarmEntity, EntityAlarmCompositeKey> {

    List<EntityAlarmEntity> findAllByAlarmId(UUID alarmId);

    @Transactional
    @Modifying
    @Query("DELETE FROM EntityAlarmEntity e where e.entityId = :entityId")
    int deleteByEntityId(@Param("entityId") UUID entityId);

    @Transactional
    @Modifying
    @Query("DELETE FROM EntityAlarmEntity a WHERE a.tenantId = :tenantId")
    void deleteByTenantId(@Param("tenantId") UUID tenantId);

    List<EntityAlarmEntity> findAllByEntityId(UUID entityId);

    Page<EntityAlarmEntity> findByTenantId(UUID tenantId, Pageable pageable);

}
