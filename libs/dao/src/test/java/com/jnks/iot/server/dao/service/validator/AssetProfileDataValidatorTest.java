package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.asset.AssetProfileDao;
import com.jnks.iot.server.dao.asset.AssetProfileService;
import com.jnks.iot.server.dao.dashboard.DashboardService;
import com.jnks.iot.server.dao.queue.QueueService;
import com.jnks.iot.server.dao.rule.RuleChainService;
import com.jnks.iot.server.dao.tenant.TenantService;

import java.util.UUID;

import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = AssetProfileDataValidator.class)
class AssetProfileDataValidatorTest {

    @MockBean
    AssetProfileDao assetProfileDao;
    @MockBean
    AssetProfileService assetProfileService;
    @MockBean
    TenantService tenantService;
    @MockBean
    QueueService queueService;
    @MockBean
    RuleChainService ruleChainService;
    @MockBean
    DashboardService dashboardService;
    @SpyBean
    AssetProfileDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @BeforeEach
    void setUp() {
        willReturn(true).given(tenantService).tenantExists(tenantId);
    }

    @Test
    void testValidateNameInvocation() {
        AssetProfile assetProfile = new AssetProfile();
        assetProfile.setName("prod");
        assetProfile.setTenantId(tenantId);

        validator.validateDataImpl(tenantId, assetProfile);
        verify(validator).validateString("Asset profile name", assetProfile.getName());
    }

}