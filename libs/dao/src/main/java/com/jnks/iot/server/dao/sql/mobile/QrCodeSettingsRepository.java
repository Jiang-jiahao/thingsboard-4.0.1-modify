package com.jnks.iot.server.dao.sql.mobile;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import com.jnks.iot.server.dao.model.sql.QrCodeSettingsEntity;

import java.util.UUID;


public interface QrCodeSettingsRepository extends JpaRepository<QrCodeSettingsEntity, UUID> {

    QrCodeSettingsEntity findByTenantId(@Param("tenantId") UUID tenantId);

    @Transactional
    @Modifying
    @Query("DELETE FROM QrCodeSettingsEntity r WHERE r.tenantId = :tenantId")
    void deleteByTenantId(@Param("tenantId") UUID tenantId);
}
