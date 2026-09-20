package com.jnks.iot.server.dao.service.validator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.cf.CalculatedFieldLinkDao;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;

@Component
public class CalculatedFieldLinkDataValidator extends DataValidator<CalculatedFieldLink> {

    @Autowired
    private CalculatedFieldLinkDao calculatedFieldLinkDao;

    @Override
    protected CalculatedFieldLink validateUpdate(TenantId tenantId, CalculatedFieldLink calculatedFieldLink) {
        CalculatedFieldLink old = calculatedFieldLinkDao.findById(calculatedFieldLink.getTenantId(), calculatedFieldLink.getId().getId());
        if (old == null) {
            throw new DataValidationException("Can't update non existing calculated field link!");
        }
        return old;
    }

}
