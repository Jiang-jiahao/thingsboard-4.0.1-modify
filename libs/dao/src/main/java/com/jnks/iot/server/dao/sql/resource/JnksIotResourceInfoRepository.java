package com.jnks.iot.server.dao.sql.resource;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import com.jnks.iot.server.dao.model.sql.JnksIotResourceInfoEntity;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface JnksIotResourceInfoRepository extends JpaRepository<JnksIotResourceInfoEntity, UUID> {

    @Query("SELECT tr FROM JnksIotResourceInfoEntity tr WHERE " +
            "(:searchText IS NULL OR ilike(tr.title, CONCAT('%', :searchText, '%')) = true) " +
            "AND (tr.tenantId = :tenantId " +
            "OR (tr.tenantId = :systemTenantId " +
            "AND NOT EXISTS " +
            "(SELECT sr FROM JnksIotResourceEntity sr " +
            "WHERE sr.tenantId = :tenantId " +
            "AND tr.resourceType = sr.resourceType " +
            "AND tr.resourceKey = sr.resourceKey)))" +
            "AND tr.resourceType IN :resourceTypes " +
            "AND (:resourceSubTypes IS NULL OR tr.resourceSubType IN :resourceSubTypes)")
    Page<JnksIotResourceInfoEntity> findAllTenantResourcesByTenantId(@Param("tenantId") UUID tenantId,
                                                                @Param("systemTenantId") UUID systemTenantId,
                                                                @Param("resourceTypes") List<String> resourceTypes,
                                                                @Param("resourceSubTypes") List<String> resourceSubTypes,
                                                                @Param("searchText") String searchText,
                                                                Pageable pageable);

    @Query("SELECT ri FROM JnksIotResourceInfoEntity ri WHERE " +
            "ri.tenantId = :tenantId " +
            "AND ri.resourceType IN :resourceTypes " +
            "AND (:resourceSubTypes IS NULL OR ri.resourceSubType IN :resourceSubTypes) " +
            "AND (:searchText IS NULL OR ilike(ri.title, CONCAT('%', :searchText, '%')) = true)")
    Page<JnksIotResourceInfoEntity> findTenantResourcesByTenantId(@Param("tenantId") UUID tenantId,
                                                             @Param("resourceTypes") List<String> resourceTypes,
                                                             @Param("resourceSubTypes") List<String> resourceSubTypes,
                                                             @Param("searchText") String searchText,
                                                             Pageable pageable);

    JnksIotResourceInfoEntity findByTenantIdAndResourceTypeAndResourceKey(UUID tenantId, String resourceType, String resourceKey);

    boolean existsByTenantIdAndResourceTypeAndResourceKey(UUID tenantId, String resourceType, String resourceKey);

    @Query(value = "SELECT r.resource_key FROM resource r WHERE r.tenant_id = :tenantId AND r.resource_type = :resourceType " +
            "AND starts_with(r.resource_key, :prefix)", nativeQuery = true)
    Set<String> findKeysByTenantIdAndResourceTypeAndResourceKeyStartingWith(@Param("tenantId") UUID tenantId,
                                                                            @Param("resourceType") String resourceType,
                                                                            @Param("prefix") String prefix);

    List<JnksIotResourceInfoEntity> findByTenantIdAndEtagAndResourceKeyStartingWith(UUID tenantId, String etag, String query);

    @Query(value = "SELECT * FROM resource r WHERE (r.tenant_id = '13814000-1dd2-11b2-8080-808080808080' OR r.tenant_id = :tenantId) " +
            "AND r.resource_type = :resourceType AND r.etag = :etag ORDER BY created_time, id LIMIT 1", nativeQuery = true)
    JnksIotResourceInfoEntity findSystemOrTenantResourceByEtag(@Param("tenantId") UUID tenantId,
                                                          @Param("resourceType") String resourceType,
                                                          @Param("etag") String etag);

    boolean existsByResourceTypeAndPublicResourceKey(String resourceType, String publicResourceKey);

    JnksIotResourceInfoEntity findByResourceTypeAndPublicResourceKeyAndIsPublicTrue(String resourceType, String publicResourceKey);

}
