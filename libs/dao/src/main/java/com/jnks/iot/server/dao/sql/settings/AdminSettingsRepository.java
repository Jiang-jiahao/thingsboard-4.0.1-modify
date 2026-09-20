package com.jnks.iot.server.dao.sql.settings;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import com.jnks.iot.server.dao.model.sql.AdminSettingsEntity;

import java.util.UUID;

/**
 * Created by Valerii Sosliuk on 5/6/2017.
 */
public interface AdminSettingsRepository extends JpaRepository<AdminSettingsEntity, UUID> {

    AdminSettingsEntity findByTenantIdAndKey(UUID tenantId, String key);

    void deleteByTenantIdAndKey(UUID tenantId, String key);

    void deleteByTenantId(UUID tenantId);

    boolean existsByTenantIdAndKey(UUID tenantId, String key);

    Page<AdminSettingsEntity> findByTenantId(UUID tenantId, Pageable pageable);

}
