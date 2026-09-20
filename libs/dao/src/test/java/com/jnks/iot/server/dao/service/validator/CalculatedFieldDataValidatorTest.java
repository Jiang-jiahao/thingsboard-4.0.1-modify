package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.cf.CalculatedField;
import com.jnks.iot.server.common.data.cf.CalculatedFieldType;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.cf.CalculatedFieldDao;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.usagerecord.DefaultApiLimitService;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

@SpringBootTest(classes = CalculatedFieldDataValidator.class)
public class CalculatedFieldDataValidatorTest {

    private final TenantId TENANT_ID = TenantId.fromUUID(UUID.fromString("7b5229e9-166e-41a9-a257-3b1dafad1b04"));
    private final CalculatedFieldId CALCULATED_FIELD_ID = new CalculatedFieldId(UUID.fromString("060fbe45-fbb2-4549-abf3-f72a6be3cb9f"));

    @MockBean
    private CalculatedFieldDao calculatedFieldDao;
    @MockBean
    private DefaultApiLimitService apiLimitService;
    @SpyBean
    private CalculatedFieldDataValidator validator;

    @Test
    public void testUpdateNonExistingCalculatedField() {
        CalculatedField calculatedField = new CalculatedField(CALCULATED_FIELD_ID);
        calculatedField.setType(CalculatedFieldType.SIMPLE);
        calculatedField.setName("Test");

        given(calculatedFieldDao.findById(TENANT_ID, CALCULATED_FIELD_ID.getId())).willReturn(null);

        assertThatThrownBy(() -> validator.validateUpdate(TENANT_ID, calculatedField))
                .isInstanceOf(DataValidationException.class)
                .hasMessage("Can't update non existing calculated field!");
    }

}
