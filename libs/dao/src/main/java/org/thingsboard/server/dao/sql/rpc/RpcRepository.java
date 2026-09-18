package org.thingsboard.server.dao.sql.rpc;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.thingsboard.server.common.data.rpc.RpcStatus;
import org.thingsboard.server.dao.model.sql.RpcEntity;

import java.util.UUID;

public interface RpcRepository extends JpaRepository<RpcEntity, UUID> {
    Page<RpcEntity> findAllByTenantIdAndDeviceId(UUID tenantId, UUID deviceId, Pageable pageable);

    Page<RpcEntity> findAllByTenantIdAndDeviceIdAndStatus(UUID tenantId, UUID deviceId, RpcStatus status, Pageable pageable);

    Page<RpcEntity> findAllByTenantId(UUID tenantId, Pageable pageable);

    @Modifying
    @Query(value = "DELETE FROM rpc WHERE tenant_id = :tenantId AND created_time < :expirationTime",
            nativeQuery = true)
    int deleteOutdatedRpcByTenantId(@Param("tenantId") UUID tenantId, @Param("expirationTime") Long expirationTime);
}
