package com.jnks.iot.server.dao.service.validator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.customer.CustomerDao;
import com.jnks.iot.server.dao.customer.CustomerServiceImpl;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.tenant.TenantService;

@Component
public class CustomerDataValidator extends DataValidator<Customer> {

    @Autowired
    private CustomerDao customerDao;

    @Autowired
    private TenantService tenantService;

    @Override
    protected void validateCreate(TenantId tenantId, Customer customer) {
        validateNumberOfEntitiesPerTenant(tenantId, EntityType.CUSTOMER);
    }

    @Override
    protected Customer validateUpdate(TenantId tenantId, Customer customer) {
        Customer old = customerDao.findById(customer.getTenantId(), customer.getId().getId());
        if (old == null) {
            throw new DataValidationException("Can't update non existing customer!");
        }
        return old;
    }

    @Override
    protected void validateDataImpl(TenantId tenantId, Customer customer) {
        validateString("Customer title", customer.getTitle());
        if (customer.getTitle().equals(CustomerServiceImpl.PUBLIC_CUSTOMER_TITLE)) {
            throw new DataValidationException("'Public' title for customer is system reserved!");
        }
        if (!StringUtils.isEmpty(customer.getEmail())) {
            validateEmail(customer.getEmail());
        }
        if (customer.getTenantId() == null) {
            throw new DataValidationException("Customer should be assigned to tenant!");
        } else {
            if (!tenantService.tenantExists(customer.getTenantId())) {
                throw new DataValidationException("Customer is referencing to non-existent tenant!");
            }
        }
    }
}
