package com.jnks.iot.server.dao.sql.resource;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import com.jnks.iot.server.dao.ExportableEntityRepository;
import com.jnks.iot.server.dao.model.sql.JnksIotResourceEntity;

import java.util.List;
import java.util.UUID;

public interface JnksIotResourceRepository extends JpaRepository<JnksIotResourceEntity, UUID>, ExportableEntityRepository<JnksIotResourceEntity> {

    JnksIotResourceEntity findByTenantIdAndResourceTypeAndResourceKey(UUID tenantId, String resourceType, String resourceKey);

    Page<JnksIotResourceEntity> findAllByTenantId(UUID tenantId, Pageable pageable);

    @Query("SELECT tr FROM JnksIotResourceEntity tr " +
            "WHERE tr.resourceType = :resourceType " +
            "AND (:resourceSubType IS NULL OR tr.resourceSubType = :resourceSubType) " +
            "AND (:searchText IS NULL OR ilike(tr.searchText, CONCAT('%', :searchText, '%')) = true) " +
            "AND (tr.tenantId = :tenantId " +
            "OR (tr.tenantId = :systemAdminId " +
            "AND NOT EXISTS " +
            "(SELECT sr FROM JnksIotResourceEntity sr " +
            "WHERE sr.tenantId = :tenantId " +
            "AND sr.resourceType = :resourceType " +
            "AND tr.resourceKey = sr.resourceKey)))")
    Page<JnksIotResourceEntity> findResourcesPage(
            @Param("tenantId") UUID tenantId,
            @Param("systemAdminId") UUID sysAdminId,
            @Param("resourceType") String resourceType,
            @Param("resourceSubType") String resourceSubType,
            @Param("searchText") String searchText,
            Pageable pageable);

    @Query("SELECT tr FROM JnksIotResourceEntity tr " +
            "WHERE tr.resourceType = :resourceType " +
            "AND (:resourceSubType IS NULL OR tr.resourceSubType = :resourceSubType) " +
            "AND (:searchText IS NULL OR ilike(tr.searchText, CONCAT('%', :searchText, '%')) = true) " +
            "AND (tr.tenantId = :tenantId " +
            "OR (tr.tenantId = :systemAdminId " +
            "AND NOT EXISTS " +
            "(SELECT sr FROM JnksIotResourceEntity sr " +
            "WHERE sr.tenantId = :tenantId " +
            "AND sr.resourceType = :resourceType " +
            "AND tr.resourceKey = sr.resourceKey)))")
    List<JnksIotResourceEntity> findResources(@Param("tenantId") UUID tenantId,
                                         @Param("systemAdminId") UUID sysAdminId,
                                         @Param("resourceType") String resourceType,
                                         @Param("resourceSubType") String resourceSubType,
                                         @Param("searchText") String searchText);

    @Query("SELECT tr FROM JnksIotResourceEntity tr " +
            "WHERE tr.resourceType = :resourceType " +
            "AND tr.resourceKey in (:resourceIds) " +
            "AND (tr.tenantId = :tenantId " +
            "OR (tr.tenantId = :systemAdminId " +
            "AND NOT EXISTS " +
            "(SELECT sr FROM JnksIotResourceEntity sr " +
            "WHERE sr.tenantId = :tenantId " +
            "AND sr.resourceType = :resourceType " +
            "AND tr.resourceKey = sr.resourceKey)))")
    List<JnksIotResourceEntity> findResourcesByIds(@Param("tenantId") UUID tenantId,
                                              @Param("systemAdminId") UUID sysAdminId,
                                              @Param("resourceType") String resourceType,
                                              @Param("resourceIds") String[] objectIds);

    @Query(value = "SELECT COALESCE(SUM(LENGTH(r.data)), 0) FROM resource r WHERE r.tenant_id = :tenantId", nativeQuery = true)
    Long sumDataSizeByTenantId(@Param("tenantId") UUID tenantId);

    @Query("SELECT r.data FROM JnksIotResourceEntity r WHERE r.id = :id")
    byte[] getDataById(@Param("id") UUID id);

    @Query(value = "SELECT COALESCE(preview, data) FROM resource WHERE id = :id", nativeQuery = true)
    byte[] getPreviewById(@Param("id") UUID id);

    @Query(value = "SELECT length(r.data) FROM resource r WHERE r.id = :id", nativeQuery = true)
    long getDataSizeById(@Param("id") UUID id);

    @Query("SELECT externalId FROM JnksIotResourceInfoEntity WHERE id = :id")
    UUID getExternalIdByInternal(@Param("id") UUID internalId);

    @Query("SELECT r.id FROM JnksIotResourceInfoEntity r WHERE r.tenantId = :tenantId")
    Page<UUID> findIdsByTenantId(@Param("tenantId") UUID tenantId, Pageable pageable);

}
