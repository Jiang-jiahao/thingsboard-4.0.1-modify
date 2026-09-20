package com.jnks.iot.server.dao.service.validator;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.widget.WidgetType;
import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.service.DataValidator;
import com.jnks.iot.server.dao.tenant.TenantService;
import com.jnks.iot.server.dao.widget.WidgetTypeDao;
import com.jnks.iot.server.dao.widget.WidgetsBundleDao;

@Component
@AllArgsConstructor
public class WidgetTypeDataValidator extends DataValidator<WidgetTypeDetails> {

    private final WidgetTypeDao widgetTypeDao;
    private final WidgetsBundleDao widgetsBundleDao;
    private final TenantService tenantService;

    @Override
    protected void validateDataImpl(TenantId tenantId, WidgetTypeDetails widgetTypeDetails) {
        validateString("Widgets type name", widgetTypeDetails.getName());
        if (widgetTypeDetails.getDescriptor() == null || widgetTypeDetails.getDescriptor().size() == 0) {
            throw new DataValidationException("Widgets type descriptor can't be empty!");
        }
        if (widgetTypeDetails.getTenantId() == null) {
            widgetTypeDetails.setTenantId(TenantId.fromUUID(ModelConstants.NULL_UUID));
        }
        if (!widgetTypeDetails.getTenantId().getId().equals(ModelConstants.NULL_UUID)) {
            if (!tenantService.tenantExists(widgetTypeDetails.getTenantId())) {
                throw new DataValidationException("Widget type is referencing to non-existent tenant!");
            }
        }
    }

    @Override
    protected void validateCreate(TenantId tenantId, WidgetTypeDetails widgetTypeDetails) {
        String fqn = widgetTypeDetails.getFqn();
        if (fqn == null || fqn.trim().isEmpty()) {
            fqn = widgetTypeDetails.getName().toLowerCase().replaceAll("\\W+", "_");
        }
        String originalFqn = fqn;
        int c = 1;
        WidgetType withSameFqn;
        do {
            withSameFqn = widgetTypeDao.findByTenantIdAndFqn(widgetTypeDetails.getTenantId().getId(), fqn);
            if (withSameFqn != null) {
                fqn = originalFqn + (++c);
            }
        } while (withSameFqn != null);
        widgetTypeDetails.setFqn(fqn);
    }

    @Override
    protected WidgetTypeDetails validateUpdate(TenantId tenantId, WidgetTypeDetails widgetTypeDetails) {
        WidgetTypeDetails storedWidgetType = widgetTypeDao.findById(tenantId, widgetTypeDetails.getId().getId());
        if (!storedWidgetType.getTenantId().getId().equals(widgetTypeDetails.getTenantId().getId())) {
            throw new DataValidationException("Can't move existing widget type to different tenant!");
        }
        if (!storedWidgetType.getFqn().equals(widgetTypeDetails.getFqn())) {
            throw new DataValidationException("Update of widget type fqn is prohibited!");
        }
        return new WidgetTypeDetails(storedWidgetType);
    }
}
