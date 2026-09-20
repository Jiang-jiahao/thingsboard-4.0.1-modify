package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.tenant.TenantDao;

import java.util.UUID;

import static org.mockito.Mockito.verify;

@SpringBootTest(classes = TenantDataValidator.class)
class TenantDataValidatorTest {

    @MockBean
    TenantDao tenantDao;
    @SpyBean
    TenantDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @Test
    void testValidateNameInvocation() {
        Tenant tenant = new Tenant();
        tenant.setTitle("Monster corporation ©");
        tenant.setEmail("support@iot.example.com");

        validator.validateDataImpl(tenantId, tenant);
        verify(validator).validateString("Tenant title", tenant.getTitle());
    }

}
