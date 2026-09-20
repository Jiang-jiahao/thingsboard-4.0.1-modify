package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import com.jnks.iot.server.common.data.cf.CalculatedFieldLink;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.CalculatedFieldLinkId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.cf.CalculatedFieldLinkDao;
import com.jnks.iot.server.dao.exception.DataValidationException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

@SpringBootTest(classes = CalculatedFieldLinkDataValidator.class)
public class CalculatedFieldLinkDataValidatorTest {

    private final TenantId TENANT_ID = TenantId.fromUUID(UUID.fromString("2ba09d99-6143-43dc-b645-381fc0c43ebe"));
    private final CalculatedFieldLinkId CALCULATED_FIELD_LINK_ID = new CalculatedFieldLinkId(UUID.fromString("a5609ef4-cb42-43ce-9b23-e090a4878d1c"));

    @MockBean
    private CalculatedFieldLinkDao calculatedFieldLinkDao;
    @SpyBean
    private CalculatedFieldLinkDataValidator validator;

    @Test
    public void testUpdateNonExistingCalculatedField() {
        CalculatedFieldLink calculatedFieldLink = new CalculatedFieldLink(CALCULATED_FIELD_LINK_ID);
        calculatedFieldLink.setCalculatedFieldId(new CalculatedFieldId(UUID.fromString("136477af-fd07-4498-b9c9-54fe50e82992")));

        given(calculatedFieldLinkDao.findById(TENANT_ID, CALCULATED_FIELD_LINK_ID.getId())).willReturn(null);

        assertThatThrownBy(() -> validator.validateUpdate(TENANT_ID, calculatedFieldLink))
                .isInstanceOf(DataValidationException.class)
                .hasMessage("Can't update non existing calculated field link!");
    }

}
