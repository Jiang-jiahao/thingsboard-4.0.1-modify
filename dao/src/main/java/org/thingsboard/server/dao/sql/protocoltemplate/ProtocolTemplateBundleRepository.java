/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.dao.sql.protocoltemplate;

import org.springframework.data.jpa.repository.JpaRepository;
import org.thingsboard.server.dao.model.sql.ProtocolTemplateBundleEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ProtocolTemplateBundleRepository extends JpaRepository<ProtocolTemplateBundleEntity, UUID> {

    List<ProtocolTemplateBundleEntity> findAllByTenantIdOrderByCreatedTimeAsc(UUID tenantId);

    Optional<ProtocolTemplateBundleEntity> findByTenantIdAndId(UUID tenantId, UUID id);

    void deleteByTenantIdAndId(UUID tenantId, UUID id);

    long countByTenantId(UUID tenantId);
}
