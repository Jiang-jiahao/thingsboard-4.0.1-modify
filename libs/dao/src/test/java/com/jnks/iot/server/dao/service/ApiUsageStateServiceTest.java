package com.jnks.iot.server.dao.service;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.ApiUsageStateValue;
import com.jnks.iot.server.dao.exception.IncorrectParameterException;
import com.jnks.iot.server.dao.usagerecord.ApiUsageStateService;


@DaoSqlTest
public class ApiUsageStateServiceTest extends AbstractServiceTest {

    @Autowired
    ApiUsageStateService apiUsageStateService;

    @Test
    public void testFindTenantApiUsageState() {
        ApiUsageState state = apiUsageStateService.findTenantApiUsageState(tenantId);
        Assert.assertNotNull(state);
    }

    @Test
    public void testUpdate() {
        ApiUsageState state = apiUsageStateService.findTenantApiUsageState(tenantId);

        state.setTransportState(ApiUsageStateValue.DISABLED);
        ApiUsageState updated = apiUsageStateService.update(state);
        Assert.assertEquals(ApiUsageStateValue.DISABLED, updated.getTransportState());
    }

    @Test
    public void testUpdateWithNullId() {
        ApiUsageState newState = new ApiUsageState();
        newState.setTenantId(tenantId);
        newState.setTransportState(ApiUsageStateValue.ENABLED);
        Assert.assertThrows(IncorrectParameterException.class, () -> apiUsageStateService.update(newState));
    }

    @Test
    public void testFindApiUsageStateByEntityId() {
        ApiUsageState state = apiUsageStateService.findApiUsageStateByEntityId(tenantId);
        Assert.assertNotNull(state);
    }

    @Test
    public void testDeleteByTenantId() {
        ApiUsageState state = apiUsageStateService.findTenantApiUsageState(tenantId);
        Assert.assertNotNull(state);

        apiUsageStateService.deleteByTenantId(tenantId);
        state = apiUsageStateService.findTenantApiUsageState(tenantId);
        Assert.assertNull(state);
    }

}
