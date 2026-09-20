package com.jnks.iot.server.dao.sql.tenant;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import com.jnks.iot.server.common.data.EntityInfo;
import com.jnks.iot.server.common.data.edqs.fields.TenantProfileFields;
import com.jnks.iot.server.dao.model.sql.TenantProfileEntity;

import java.util.List;
import java.util.UUID;

public interface TenantProfileRepository extends JpaRepository<TenantProfileEntity, UUID> {

    @Query("SELECT new com.jnks.iot.server.common.data.EntityInfo(t.id, 'TENANT_PROFILE', t.name) " +
            "FROM TenantProfileEntity t " +
            "WHERE t.id = :tenantProfileId")
    EntityInfo findTenantProfileInfoById(@Param("tenantProfileId") UUID tenantProfileId);

    @Query("SELECT t FROM TenantProfileEntity t WHERE " +
            "(:textSearch IS NULL OR ilike(t.name, CONCAT('%', :textSearch, '%')) = true)")
    Page<TenantProfileEntity> findTenantProfiles(@Param("textSearch") String textSearch,
                                                 Pageable pageable);

    @Query("SELECT new com.jnks.iot.server.common.data.EntityInfo(t.id, 'TENANT_PROFILE', t.name) " +
            "FROM TenantProfileEntity t " +
            "WHERE (:textSearch IS NULL OR ilike(t.name, CONCAT('%', :textSearch, '%')) = true)")
    Page<EntityInfo> findTenantProfileInfos(@Param("textSearch") String textSearch,
                                            Pageable pageable);

    @Query("SELECT t FROM TenantProfileEntity t " +
            "WHERE t.isDefault = true")
    TenantProfileEntity findByDefaultTrue();

    @Query("SELECT new com.jnks.iot.server.common.data.EntityInfo(t.id, 'TENANT_PROFILE', t.name) " +
            "FROM TenantProfileEntity t " +
            "WHERE t.isDefault = true")
    EntityInfo findDefaultTenantProfileInfo();

    List<TenantProfileEntity> findByIdIn(List<UUID> ids);

    @Query("SELECT new com.jnks.iot.server.common.data.edqs.fields.TenantProfileFields(t.id, t.createdTime, t.name," +
            "t.isDefault) FROM TenantProfileEntity t WHERE t.id > :id ORDER BY t.id")
    List<TenantProfileFields> findNextBatch(@Param("id") UUID id, Limit limit);

}
