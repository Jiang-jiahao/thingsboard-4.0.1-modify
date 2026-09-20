package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import com.jnks.iot.server.common.data.tenant.profile.TenantProfileData;
import com.jnks.iot.server.dao.tenant.TenantProfileDao;
import com.jnks.iot.server.dao.tenant.TenantProfileService;

import java.util.UUID;

import static org.mockito.Mockito.verify;

@SpringBootTest(classes = TenantProfileDataValidator.class)
class TenantProfileDataValidatorTest {

    @MockBean
    TenantProfileDao tenantProfileDao;
    @MockBean
    TenantProfileService tenantProfileService;
    @SpyBean
    TenantProfileDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @Test
    void testValidateNameInvocation() {
        TenantProfile tenantProfile = new TenantProfile();
        tenantProfile.setName("Sandbox");
        TenantProfileData tenantProfileData = new TenantProfileData();
        tenantProfileData.setConfiguration(new DefaultTenantProfileConfiguration());
        tenantProfile.setProfileData(tenantProfileData);

        validator.validateDataImpl(tenantId, tenantProfile);
        verify(validator).validateString("Tenant profile name", tenantProfile.getName());
    }

}
