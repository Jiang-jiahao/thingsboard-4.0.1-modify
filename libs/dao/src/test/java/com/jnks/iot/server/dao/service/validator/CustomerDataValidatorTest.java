package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.customer.CustomerDao;
import com.jnks.iot.server.dao.tenant.TenantService;

import java.util.UUID;

import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = CustomerDataValidator.class)
class CustomerDataValidatorTest {

    @MockBean
    CustomerDao customerDao;
    @MockBean
    TenantService tenantService;
    @SpyBean
    CustomerDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @BeforeEach
    void setUp() {
        willReturn(true).given(tenantService).tenantExists(tenantId);
    }

    @Test
    void testValidateNameInvocation() {
        Customer customer = new Customer();
        customer.setTitle("Customer A");
        customer.setTenantId(tenantId);

        validator.validateDataImpl(tenantId, customer);
        verify(validator).validateString("Customer title", customer.getTitle());
    }

}
