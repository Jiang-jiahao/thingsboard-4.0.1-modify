package org.thingsboard.server.dao.service.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.data.rule.RuleChainType;
import org.thingsboard.server.dao.rule.RuleChainService;
import org.thingsboard.server.dao.tenant.TenantService;

import java.util.UUID;

import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = RuleChainDataValidator.class)
class RuleChainDataValidatorTest {

    @MockBean
    RuleChainService ruleChainService;
    @MockBean
    TenantService tenantService;
    @SpyBean
    RuleChainDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @BeforeEach
    void setUp() {
        willReturn(true).given(tenantService).tenantExists(tenantId);
    }

    @Test
    void testValidateNameInvocation() {
        RuleChain ruleChain = new RuleChain();
        ruleChain.setName("generate daily report");
        ruleChain.setType(RuleChainType.CORE);
        ruleChain.setTenantId(tenantId);

        validator.validateDataImpl(tenantId, ruleChain);
        verify(validator).validateString("Rule chain name", ruleChain.getName());
    }

}
