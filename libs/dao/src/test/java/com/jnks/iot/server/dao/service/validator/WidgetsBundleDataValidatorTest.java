package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.widget.WidgetsBundle;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.dao.widget.WidgetsBundleDao;

import java.util.UUID;

import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = WidgetsBundleDataValidator.class)
class WidgetsBundleDataValidatorTest {

    @MockBean
    WidgetsBundleDao widgetsBundleDao;
    @MockBean
    TenantService tenantService;
    @SpyBean
    WidgetsBundleDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @BeforeEach
    void setUp() {
        willReturn(true).given(tenantService).tenantExists(tenantId);
    }

    @Test
    void testValidateNameInvocation() {
        WidgetsBundle widgetsBundle = new WidgetsBundle();
        widgetsBundle.setTitle("my fancy WB");
        widgetsBundle.setTenantId(tenantId);

        validator.validateDataImpl(tenantId, widgetsBundle);
        verify(validator).validateString("Widgets bundle title", widgetsBundle.getTitle());
    }

}
