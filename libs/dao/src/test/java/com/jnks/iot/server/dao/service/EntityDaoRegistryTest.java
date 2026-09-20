package com.jnks.iot.server.dao.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.Dao;
import com.jnks.iot.server.dao.entity.EntityDaoRegistry;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Slf4j
@DaoSqlTest
public class EntityDaoRegistryTest extends AbstractServiceTest {

    @Autowired
    EntityDaoRegistry entityDaoRegistry;

    @Autowired
    List<JpaRepository<?, ?>> repositories;

    @Test
    public void givenAllEntityTypes_whenGetDao_thenAllPresent() {
        for (EntityType entityType : EntityType.values()) {
            Dao<?> dao = assertDoesNotThrow(() -> entityDaoRegistry.getDao(entityType));
            assertThat(dao).isNotNull();
        }
    }

    @Test
    public void givenAllDaos_whenFindById_thenOk() {
        for (EntityType entityType : EntityType.values()) {
            Dao<?> dao = entityDaoRegistry.getDao(entityType);
            assertDoesNotThrow(() -> {
                dao.findById(TenantId.SYS_TENANT_ID, UUID.randomUUID());
            });
        }
    }

    @Test
    public void givenAllDaos_whenFindIdsByTenantIdAndIdOffset_thenOk() {
        for (EntityType entityType : EntityType.values()) {
            Dao<?> dao = entityDaoRegistry.getDao(entityType);
            try {
                dao.findIdsByTenantIdAndIdOffset(TenantId.SYS_TENANT_ID, null, 10);
                dao.findIdsByTenantIdAndIdOffset(TenantId.SYS_TENANT_ID, UUID.randomUUID(), 10);
            } catch (Exception e) {
                String error = ExceptionUtils.getRootCauseMessage(e);
                if (error.contains("tenant_id")) {
                    log.debug("[{}] Ignoring not found tenant_id column", entityType);
                } else {
                    fail("findIdsByTenantIdAndIdOffset for " + entityType + " dao threw error: " + error);
                }
            }
        }
    }

    /*
     * Verifying that all the repositories are successfully bootstrapped, when using Lazy Jpa bootstrap mode
     * */
    @Test
    public void testJpaRepositories() {
        for (var repository : repositories) {
            repository.count();
        }
    }

}
