package com.jnks.iot.server.dao.service.validator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.security.Authority;
import com.jnks.iot.server.dao.customer.CustomerDao;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.dao.user.UserDao;
import com.jnks.iot.server.dao.user.UserService;

@Component
public class UserDataValidator extends DataValidator<User> {

    @Autowired
    private UserDao userDao;

    @Autowired
    @Lazy
    private UserService userService;

    @Autowired
    private CustomerDao customerDao;

    @Autowired
    @Lazy
    private TenantService tenantService;

    @Override
    protected void validateCreate(TenantId tenantId, User user) {
        if (!user.getTenantId().getId().equals(ModelConstants.NULL_UUID)) {
            validateNumberOfEntitiesPerTenant(tenantId, EntityType.USER);
        }
    }

    @Override
    protected User validateUpdate(TenantId tenantId, User user) {
        User old = userDao.findById(user.getTenantId(), user.getId().getId());
        if (old == null) {
            throw new DataValidationException("Can't update non existing user!");
        }
        if (!old.getTenantId().equals(user.getTenantId())) {
            throw new DataValidationException("Can't update user tenant id!");
        }
        if (!old.getAuthority().equals(user.getAuthority())) {
            throw new DataValidationException("Can't update user authority!");
        }
        if (!old.getCustomerId().equals(user.getCustomerId())) {
            throw new DataValidationException("Can't update user customer id!");
        }
        return old;
    }

    @Override
    protected void validateDataImpl(TenantId requestTenantId, User user) {
        if (StringUtils.isEmpty(user.getEmail())) {
            throw new DataValidationException("User email should be specified!");
        }

        validateEmail(user.getEmail());

        Authority authority = user.getAuthority();
        if (authority == null) {
            throw new DataValidationException("User authority isn't defined!");
        }
        TenantId tenantId = user.getTenantId();
        if (tenantId == null) {
            tenantId = TenantId.fromUUID(ModelConstants.NULL_UUID);
            user.setTenantId(tenantId);
        }
        CustomerId customerId = user.getCustomerId();
        if (customerId == null) {
            customerId = new CustomerId(ModelConstants.NULL_UUID);
            user.setCustomerId(customerId);
        }

        switch (authority) {
            case SYS_ADMIN:
                if (!tenantId.getId().equals(ModelConstants.NULL_UUID)
                        || !customerId.getId().equals(ModelConstants.NULL_UUID)) {
                    throw new DataValidationException("System administrator can't be assigned neither to tenant nor to customer!");
                }
                break;
            case TENANT_ADMIN:
                if (tenantId.getId().equals(ModelConstants.NULL_UUID)) {
                    throw new DataValidationException("Tenant administrator should be assigned to tenant!");
                } else if (!customerId.getId().equals(ModelConstants.NULL_UUID)) {
                    throw new DataValidationException("Tenant administrator can't be assigned to customer!");
                }
                break;
            case CUSTOMER_USER:
                if (tenantId.getId().equals(ModelConstants.NULL_UUID)
                        || customerId.getId().equals(ModelConstants.NULL_UUID)) {
                    throw new DataValidationException("Customer user should be assigned to customer!");
                }
                break;
            default:
                break;
        }

        if (!tenantId.getId().equals(ModelConstants.NULL_UUID)) {
            if (!tenantService.tenantExists(user.getTenantId())) {
                throw new DataValidationException("User is referencing to non-existent tenant!");
            }
        }
        if (!customerId.getId().equals(ModelConstants.NULL_UUID)) {
            Customer customer = customerDao.findById(tenantId, user.getCustomerId().getId());
            if (customer == null) {
                throw new DataValidationException("User is referencing to non-existent customer!");
            } else if (!customer.getTenantId().getId().equals(tenantId.getId())) {
                throw new DataValidationException("User can't be assigned to customer from different tenant!");
            }
        }
    }
}
