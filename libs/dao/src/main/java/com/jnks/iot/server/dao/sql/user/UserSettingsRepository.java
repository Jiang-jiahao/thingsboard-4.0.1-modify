package com.jnks.iot.server.dao.sql.user;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import com.jnks.iot.server.common.data.settings.UserSettingsCompositeKey;
import com.jnks.iot.server.dao.model.sql.UserSettingsEntity;

import java.util.List;
import java.util.UUID;

public interface UserSettingsRepository extends JpaRepository<UserSettingsEntity, UserSettingsCompositeKey> {

    @Transactional
    @Modifying
    @Query("DELETE FROM UserSettingsEntity s WHERE s.userId = :userId")
    void deleteByUserId(@Param("userId") UUID userId);

    @Query(value = "SELECT * FROM user_settings WHERE type = :type AND (settings #> :path) IS NOT NULL", nativeQuery = true)
    List<UserSettingsEntity> findByTypeAndPathExisting(@Param("type") String type, @Param("path") String[] path);

    @Query("SELECT s FROM UserSettingsEntity s WHERE s.userId IN (SELECT u.id FROM UserEntity u WHERE u.tenantId = :tenantId)")
    Page<UserSettingsEntity> findByTenantId(@Param("tenantId") UUID tenantId, Pageable pageable);

}
