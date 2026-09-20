package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.plugin.ComponentDescriptor;
import com.jnks.iot.server.common.data.plugin.ComponentScope;
import com.jnks.iot.server.common.data.plugin.ComponentType;

import static org.mockito.Mockito.verify;

@SpringBootTest(classes = ComponentDescriptorDataValidator.class)
class ComponentDescriptorDataValidatorTest {
    @SpyBean
    ComponentDescriptorDataValidator validator;

    @Test
    void testValidateNameInvocation() {
        ComponentDescriptor plugin = new ComponentDescriptor();
        plugin.setType(ComponentType.ENRICHMENT);
        plugin.setScope(ComponentScope.SYSTEM);
        plugin.setName("originator attributes");
        plugin.setClazz("com.jnks.iot.rule.engine.metadata.TbGetAttributesNode");
        validator.validateDataImpl(TenantId.SYS_TENANT_ID, plugin);
        verify(validator).validateString("Component name", plugin.getName());
    }
}
