/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.Data;
import org.hibernate.annotations.JdbcType;
import org.hibernate.dialect.PostgreSQLJsonPGObjectJsonbType;
import org.thingsboard.server.dao.model.ModelConstants;
import org.thingsboard.server.dao.util.mapping.JsonConverter;

import java.util.UUID;

@Data
@Entity
@Table(name = ModelConstants.PROTOCOL_TEMPLATE_BUNDLE_TABLE_NAME)
public class ProtocolTemplateBundleEntity {

    @Id
    @Column(name = ModelConstants.ID_PROPERTY, columnDefinition = "uuid")
    private UUID id;

    @Column(name = ModelConstants.CREATED_TIME_PROPERTY, updatable = false)
    private long createdTime;

    @Column(name = ModelConstants.TENANT_ID_COLUMN, columnDefinition = "uuid")
    private UUID tenantId;

    @Column(name = ModelConstants.PROTOCOL_TEMPLATE_BUNDLE_NAME_PROPERTY)
    private String name;

    @Convert(converter = JsonConverter.class)
    @JdbcType(PostgreSQLJsonPGObjectJsonbType.class)
    @Column(name = ModelConstants.PROTOCOL_TEMPLATE_BUNDLE_DATA_PROPERTY, columnDefinition = "jsonb")
    private JsonNode bundleData;

    @Version
    @Column(name = ModelConstants.PROTOCOL_TEMPLATE_BUNDLE_VERSION_PROPERTY)
    private Long version;
}
