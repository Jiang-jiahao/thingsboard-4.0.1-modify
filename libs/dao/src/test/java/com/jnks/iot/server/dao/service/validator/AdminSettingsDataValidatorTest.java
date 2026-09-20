package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.AdminSettings;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.settings.AdminSettingsService;

import java.util.UUID;

import static org.mockito.Mockito.verify;

@SpringBootTest(classes = AdminSettingsDataValidator.class)
class AdminSettingsDataValidatorTest {

    @MockBean
    AdminSettingsService adminSettingsService;
    @SpyBean
    AdminSettingsDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @Test
    void testValidateNameInvocation() {
        AdminSettings adminSettings = new AdminSettings();
        adminSettings.setKey("jwt");
        adminSettings.setJsonValue(JacksonUtil.toJsonNode("{}"));

        validator.validateDataImpl(tenantId, adminSettings);
        verify(validator).validateString("Key", adminSettings.getKey());
    }

}