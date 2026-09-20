package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.ResourceType;
import com.jnks.iot.server.common.data.TbResource;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.resource.TbResourceDao;
import com.jnks.iot.server.dao.tenant.TbTenantProfileCache;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.dao.widget.WidgetTypeDao;

import java.util.UUID;

import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = ResourceDataValidator.class)
class ResourceDataValidatorTest {

    @MockBean
    TbResourceDao resourceDao;
    @MockBean
    WidgetTypeDao widgetTypeDao;
    @MockBean
    TenantService tenantService;
    @MockBean
    TbTenantProfileCache tenantProfileCache;
    @SpyBean
    ResourceDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @BeforeEach
    void setUp() {
        willReturn(true).given(tenantService).tenantExists(tenantId);
    }

    @Test
    void testValidateNameInvocation() {
        TbResource resource = new TbResource();
        resource.setTitle("rss");
        resource.setResourceType(ResourceType.PKCS_12);
        resource.setFileName("cert.pem");
        resource.setResourceKey("19_1.0");
        resource.setTenantId(tenantId);

        validator.validateDataImpl(tenantId, resource);
        verify(validator).validateString("Resource title", resource.getTitle());
    }

}
