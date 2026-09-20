package com.jnks.iot.server.dao.sql.cf;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.cf.CalculatedFieldType;
import com.jnks.iot.server.common.data.cf.configuration.CalculatedFieldConfiguration;
import com.jnks.iot.server.common.data.debug.DebugSettings;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.CalculatedFieldLinkId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Repository
@Slf4j
public class DefaultNativeCalculatedFieldRepository implements NativeCalculatedFieldRepository {

    private final String CF_COUNT_QUERY = "SELECT count(id) FROM calculated_field;";
    private final String CF_QUERY = "SELECT * FROM calculated_field ORDER BY created_time ASC LIMIT %s OFFSET %s";

    private final String CFL_COUNT_QUERY = "SELECT count(id) FROM calculated_field_link;";
    private final String CFL_QUERY = "SELECT * FROM calculated_field_link ORDER BY created_time ASC LIMIT %s OFFSET %s";

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    @Override
    public PageData<CalculatedField> findCalculatedFields(Pageable pageable) {
        return transactionTemplate.execute(status -> {
            long startTs = System.currentTimeMillis();
            int totalElements = jdbcTemplate.queryForObject(CF_COUNT_QUERY, Collections.emptyMap(), Integer.class);
            log.debug("Count query took {} ms", System.currentTimeMillis() - startTs);
            startTs = System.currentTimeMillis();
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(String.format(CF_QUERY, pageable.getPageSize(), pageable.getOffset()), Collections.emptyMap());
            log.debug("Main query took {} ms", System.currentTimeMillis() - startTs);
            int totalPages = pageable.getPageSize() > 0 ? (int) Math.ceil((float) totalElements / pageable.getPageSize()) : 1;
            boolean hasNext = pageable.getPageSize() > 0 && totalElements > pageable.getOffset() + rows.size();
            var data = rows.stream().map(row -> {

                UUID id = (UUID) row.get("id");
                long createdTime = (long) row.get("created_time");
                UUID tenantId = (UUID) row.get("tenant_id");
                EntityType entityType = EntityType.valueOf((String) row.get("entity_type"));
                UUID entityId = (UUID) row.get("entity_id");
                CalculatedFieldType type = CalculatedFieldType.valueOf((String) row.get("type"));
                String name = (String) row.get("name");
                int configurationVersion = (int) row.get("configuration_version");
                JsonNode configuration = JacksonUtil.toJsonNode((String) row.get("configuration"));
                long version = row.get("version") != null ? (long) row.get("version") : 0;
                String debugSettings = (String) row.get("debug_settings");
                Object externalIdObj = row.get("external_id");

                CalculatedField calculatedField = new CalculatedField();
                calculatedField.setId(new CalculatedFieldId(id));
                calculatedField.setCreatedTime(createdTime);
                calculatedField.setTenantId(TenantId.fromUUID(tenantId));
                calculatedField.setEntityId(EntityIdFactory.getByTypeAndUuid(entityType, entityId));
                calculatedField.setType(type);
                calculatedField.setName(name);
                calculatedField.setConfigurationVersion(configurationVersion);
                calculatedField.setConfiguration(JacksonUtil.treeToValue(configuration, CalculatedFieldConfiguration.class));
                calculatedField.setVersion(version);
                calculatedField.setDebugSettings(JacksonUtil.fromString(debugSettings, DebugSettings.class));

                return calculatedField;
            }).collect(Collectors.toList());
            return new PageData<>(data, totalPages, totalElements, hasNext);
        });
    }

    @Override
    public PageData<CalculatedFieldLink> findCalculatedFieldLinks(Pageable pageable) {
        return transactionTemplate.execute(status -> {
            long startTs = System.currentTimeMillis();
            int totalElements = jdbcTemplate.queryForObject(CFL_COUNT_QUERY, Collections.emptyMap(), Integer.class);
            log.debug("Count query took {} ms", System.currentTimeMillis() - startTs);
            startTs = System.currentTimeMillis();
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(String.format(CFL_QUERY, pageable.getPageSize(), pageable.getOffset()), Collections.emptyMap());
            log.debug("Main query took {} ms", System.currentTimeMillis() - startTs);
            int totalPages = pageable.getPageSize() > 0 ? (int) Math.ceil((float) totalElements / pageable.getPageSize()) : 1;
            boolean hasNext = pageable.getPageSize() > 0 && totalElements > pageable.getOffset() + rows.size();
            var data = rows.stream().map(row -> {

                UUID id = (UUID) row.get("id");
                long createdTime = (long) row.get("created_time");
                UUID tenantId = (UUID) row.get("tenant_id");
                EntityType entityType = EntityType.valueOf((String) row.get("entity_type"));
                UUID entityId = (UUID) row.get("entity_id");
                UUID calculatedFieldId = (UUID) row.get("calculated_field_id");
                JsonNode configuration = JacksonUtil.toJsonNode((String) row.get("configuration"));

                CalculatedFieldLink calculatedFieldLink = new CalculatedFieldLink();
                calculatedFieldLink.setId(new CalculatedFieldLinkId(id));
                calculatedFieldLink.setCreatedTime(createdTime);
                calculatedFieldLink.setTenantId(new TenantId(tenantId));
                calculatedFieldLink.setEntityId(EntityIdFactory.getByTypeAndUuid(entityType, entityId));
                calculatedFieldLink.setCalculatedFieldId(new CalculatedFieldId(calculatedFieldId));

                return calculatedFieldLink;
            }).collect(Collectors.toList());
            return new PageData<>(data, totalPages, totalElements, hasNext);
        });
    }

}
