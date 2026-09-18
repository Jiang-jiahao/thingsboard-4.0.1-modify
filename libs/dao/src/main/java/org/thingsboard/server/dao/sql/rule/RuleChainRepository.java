package org.thingsboard.server.dao.sql.rule;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.thingsboard.server.common.data.edqs.fields.RuleChainFields;
import org.thingsboard.server.common.data.rule.RuleChainType;
import org.thingsboard.server.dao.ExportableEntityRepository;
import org.thingsboard.server.dao.model.sql.RuleChainEntity;

import java.util.List;
import java.util.UUID;

public interface RuleChainRepository extends JpaRepository<RuleChainEntity, UUID>, ExportableEntityRepository<RuleChainEntity> {

    @Query("SELECT rc FROM RuleChainEntity rc WHERE rc.tenantId = :tenantId " +
            "AND (:searchText IS NULL OR ilike(rc.name, CONCAT('%', :searchText, '%')) = true)")
    Page<RuleChainEntity> findByTenantId(@Param("tenantId") UUID tenantId,
                                         @Param("searchText") String searchText,
                                         Pageable pageable);

    @Query("SELECT rc FROM RuleChainEntity rc WHERE rc.tenantId = :tenantId " +
            "AND rc.type = :type " +
            "AND (:searchText IS NULL OR ilike(rc.name, CONCAT('%', :searchText, '%')) = true)")
    Page<RuleChainEntity> findByTenantIdAndType(@Param("tenantId") UUID tenantId,
                                                @Param("type") RuleChainType type,
                                                @Param("searchText") String searchText,
                                                Pageable pageable);

    RuleChainEntity findByTenantIdAndTypeAndRootIsTrue(UUID tenantId, RuleChainType ruleChainType);

    Long countByTenantId(UUID tenantId);

    List<RuleChainEntity> findByTenantIdAndTypeAndName(UUID tenantId, RuleChainType type, String name);

    @Query("SELECT externalId FROM RuleChainEntity WHERE id = :id")
    UUID getExternalIdById(@Param("id") UUID id);

    @Query("SELECT new org.thingsboard.server.common.data.edqs.fields.RuleChainFields(r.id, r.createdTime, r.tenantId," +
            "r.name, r.version, r.additionalInfo) FROM RuleChainEntity r WHERE r.id > :id ORDER BY r.id")
    List<RuleChainFields> findNextBatch(@Param("id") UUID id, Limit limit);
}
