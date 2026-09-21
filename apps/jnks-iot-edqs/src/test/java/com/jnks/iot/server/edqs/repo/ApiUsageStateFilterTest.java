package com.jnks.iot.server.edqs.repo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.ApiUsageStateValue;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.id.ApiUsageStateId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.query.ApiUsageStateFilter;
import com.jnks.iot.server.common.data.query.EntityDataPageLink;
import com.jnks.iot.server.common.data.query.EntityDataQuery;
import com.jnks.iot.server.common.data.query.EntityDataSortOrder;
import com.jnks.iot.server.common.data.query.EntityKey;
import com.jnks.iot.server.common.data.query.EntityKeyType;
import com.jnks.iot.server.common.data.query.EntityKeyValueType;
import com.jnks.iot.server.common.data.query.FilterPredicateValue;
import com.jnks.iot.server.common.data.query.KeyFilter;
import com.jnks.iot.server.common.data.query.StringFilterPredicate;

import java.util.Arrays;
import java.util.UUID;

public class ApiUsageStateFilterTest extends AbstractEDQTest {

    @Before
    public void setUp() {
        Tenant entity = new Tenant();
        entity.setId(tenantId);
        entity.setTitle("test tenant");
        addOrUpdate(EntityType.TENANT, entity);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testFindCustomerApiUsageState() {
        UUID customerId = UUID.randomUUID();
        createCustomer(customerId, null, "Customer A");

        ApiUsageState apiUsageState = buildApiUsageState(customerId);
        addOrUpdate(EntityType.API_USAGE_STATE, apiUsageState);

        var result = repository.findEntityDataByQuery(tenantId, null, getEntityDataQuery(new CustomerId(customerId)), false);

        Assert.assertEquals(1, result.getTotalElements());
        var customer = result.getData().get(0);
        Assert.assertEquals("Customer A", customer.getLatest().get(EntityKeyType.ENTITY_FIELD).get("name").getValue());
    }

    private ApiUsageState buildApiUsageState(UUID customerId) {
        ApiUsageState apiUsageState = new ApiUsageState();
        apiUsageState.setId(new ApiUsageStateId(UUID.randomUUID()));
        apiUsageState.setTenantId(tenantId);
        apiUsageState.setEntityId(new CustomerId(customerId));
        apiUsageState.setTransportState(ApiUsageStateValue.ENABLED);
        apiUsageState.setReExecState(ApiUsageStateValue.ENABLED);
        apiUsageState.setJsExecState(ApiUsageStateValue.ENABLED);
        apiUsageState.setTbelExecState(ApiUsageStateValue.ENABLED);
        apiUsageState.setDbStorageState(ApiUsageStateValue.ENABLED);
        apiUsageState.setSmsExecState(ApiUsageStateValue.ENABLED);
        apiUsageState.setEmailExecState(ApiUsageStateValue.ENABLED);
        apiUsageState.setAlarmExecState(ApiUsageStateValue.ENABLED);
        return apiUsageState;
    }

    private static EntityDataQuery getEntityDataQuery(CustomerId customerId) {
        ApiUsageStateFilter filter = new ApiUsageStateFilter();
        filter.setCustomerId(customerId);
        var pageLink = new EntityDataPageLink(20, 0, null, new EntityDataSortOrder(new EntityKey(EntityKeyType.TIME_SERIES, "name"), EntityDataSortOrder.Direction.DESC), false);

        var entityFields = Arrays.asList(new EntityKey(EntityKeyType.ENTITY_FIELD, "name"), new EntityKey(EntityKeyType.ENTITY_FIELD, "createdTime"));
        KeyFilter nameFilter = new KeyFilter();
        nameFilter.setKey(new EntityKey(EntityKeyType.ENTITY_FIELD, "name"));
        var predicate = new StringFilterPredicate();
        predicate.setIgnoreCase(false);
        predicate.setOperation(StringFilterPredicate.StringOperation.CONTAINS);
        predicate.setValue(new FilterPredicateValue<>("Customer A"));
        nameFilter.setPredicate(predicate);
        nameFilter.setValueType(EntityKeyValueType.STRING);

        return new EntityDataQuery(filter, pageLink, entityFields, null, Arrays.asList(nameFilter));
    }

}
