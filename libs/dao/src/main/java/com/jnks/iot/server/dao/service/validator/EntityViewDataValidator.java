package com.jnks.iot.server.dao.service.validator;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.customer.CustomerDao;
import com.jnks.iot.server.dao.entityview.EntityViewDao;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.tenant.TenantService;

import static com.jnks.iot.server.dao.model.ModelConstants.NULL_UUID;

@Component
@AllArgsConstructor
public class EntityViewDataValidator extends DataValidator<EntityView> {

    private final EntityViewDao entityViewDao;
    private final TenantService tenantService;
    private final CustomerDao customerDao;

    @Override
    protected void validateCreate(TenantId tenantId, EntityView entityView) {
        entityViewDao.findEntityViewByTenantIdAndName(entityView.getTenantId().getId(), entityView.getName())
                .ifPresent(e -> {
                    throw new DataValidationException("Entity view with such name already exists!");
                });
    }

    @Override
    protected EntityView validateUpdate(TenantId tenantId, EntityView entityView) {
        var opt = entityViewDao.findEntityViewByTenantIdAndName(entityView.getTenantId().getId(), entityView.getName());
        opt.ifPresent(e -> {
            if (!e.getUuidId().equals(entityView.getUuidId())) {
                throw new DataValidationException("Entity view with such name already exists!");
            }
        });
        return opt.orElse(null);
    }

    @Override
    protected void validateDataImpl(TenantId tenantId, EntityView entityView) {
        validateString("Entity view name", entityView.getName());
        validateString("Entity view type", entityView.getType());
        if (entityView.getTenantId() == null) {
            throw new DataValidationException("Entity view should be assigned to tenant!");
        } else {
            if (!tenantService.tenantExists(entityView.getTenantId())) {
                throw new DataValidationException("Entity view is referencing to non-existent tenant!");
            }
        }
        if (entityView.getCustomerId() == null) {
            entityView.setCustomerId(new CustomerId(NULL_UUID));
        } else if (!entityView.getCustomerId().getId().equals(NULL_UUID)) {
            Customer customer = customerDao.findById(tenantId, entityView.getCustomerId().getId());
            if (customer == null) {
                throw new DataValidationException("Can't assign entity view to non-existent customer!");
            }
            if (!customer.getTenantId().getId().equals(entityView.getTenantId().getId())) {
                throw new DataValidationException("Can't assign entity view to customer from different tenant!");
            }
        }
    }
}
