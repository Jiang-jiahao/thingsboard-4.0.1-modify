package com.jnks.iot.server.edqs.repo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.LatestTsKv;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.common.data.query.EntityDataPageLink;
import com.jnks.iot.server.common.data.query.EntityDataQuery;
import com.jnks.iot.server.common.data.query.EntityDataSortOrder;
import com.jnks.iot.server.common.data.query.EntityKey;
import com.jnks.iot.server.common.data.query.EntityKeyType;
import com.jnks.iot.server.common.data.query.EntityKeyValueType;
import com.jnks.iot.server.common.data.query.EntityNameFilter;
import com.jnks.iot.server.common.data.query.FilterPredicateValue;
import com.jnks.iot.server.common.data.query.KeyFilter;
import com.jnks.iot.server.common.data.query.StringFilterPredicate;

import java.util.Arrays;
import java.util.UUID;

public class EntityNameFilterTest extends AbstractEDQTest {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testFindTenantDevice() {
        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        Device device = new Device();
        device.setId(deviceId);
        device.setTenantId(tenantId);
        device.setName("LoRa-1");
        device.setCreatedTime(42L);
        device.setDeviceProfileId(new DeviceProfileId(defaultDeviceProfileId));
        addOrUpdate(EntityType.DEVICE, device);

        var result = repository.findEntityDataByQuery(tenantId, null, getDeviceNameQuery("LoRa"), false);

        Assert.assertEquals(1, result.getTotalElements());
        var first = result.getData().get(0);
        Assert.assertEquals(deviceId, first.getEntityId());
        Assert.assertEquals("LoRa-1", first.getLatest().get(EntityKeyType.ENTITY_FIELD).get("name").getValue());
        Assert.assertEquals("42", first.getLatest().get(EntityKeyType.ENTITY_FIELD).get("createdTime").getValue());

        result = repository.findEntityDataByQuery(tenantId, null, getDeviceNameQuery("Not LoRa"), false);
        Assert.assertEquals(0, result.getTotalElements());

        device.setCustomerId(customerId);
        addOrUpdate(EntityType.DEVICE, device);

        result = repository.findEntityDataByQuery(tenantId, null, getDeviceNameQuery("%1"), false);
        Assert.assertEquals(1, result.getTotalElements());
        result = repository.findEntityDataByQuery(tenantId, null, getDeviceNameQuery("L%"), false);
        Assert.assertEquals(1, result.getTotalElements());
    }

    @Test
    public void testFindCustomerDevice() {
        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        Device device = new Device();
        device.setId(deviceId);
        device.setTenantId(tenantId);
        device.setName("LoRa-1");
        device.setCreatedTime(42L);
        device.setDeviceProfileId(new DeviceProfileId(defaultDeviceProfileId));
        addOrUpdate(EntityType.DEVICE, device);
        addOrUpdate(new LatestTsKv(deviceId, new BasicTsKvEntry(43, new StringDataEntry("state", "TEST")), 0L));

        var result = repository.findEntityDataByQuery(tenantId, customerId, getDeviceNameQuery("LoRa"), false);
        Assert.assertEquals(0, result.getTotalElements());

        device.setCustomerId(customerId);
        addOrUpdate(EntityType.DEVICE, device);

        result = repository.findEntityDataByQuery(tenantId, customerId, getDeviceNameQuery("LoRa"), false);

        Assert.assertEquals(1, result.getTotalElements());
        var first = result.getData().get(0);
        Assert.assertEquals(deviceId, first.getEntityId());
        Assert.assertEquals("LoRa-1", first.getLatest().get(EntityKeyType.ENTITY_FIELD).get("name").getValue());
        Assert.assertEquals("42", first.getLatest().get(EntityKeyType.ENTITY_FIELD).get("createdTime").getValue());
    }

    private static EntityDataQuery getDeviceNameQuery(String entityNameFilter) {
        EntityNameFilter filter = new EntityNameFilter();
        filter.setEntityType(EntityType.DEVICE);
        filter.setEntityNameFilter(entityNameFilter);
        var pageLink = new EntityDataPageLink(20, 0, null, new EntityDataSortOrder(new EntityKey(EntityKeyType.TIME_SERIES, "state"), EntityDataSortOrder.Direction.DESC), false);

        var entityFields = Arrays.asList(new EntityKey(EntityKeyType.ENTITY_FIELD, "name"), new EntityKey(EntityKeyType.ENTITY_FIELD, "createdTime"));
        var latestValues = Arrays.asList(new EntityKey(EntityKeyType.TIME_SERIES, "state"));
        KeyFilter nameFilter = new KeyFilter();
        nameFilter.setKey(new EntityKey(EntityKeyType.ENTITY_FIELD, "name"));
        var predicate = new StringFilterPredicate();
        predicate.setIgnoreCase(false);
        predicate.setOperation(StringFilterPredicate.StringOperation.CONTAINS);
        predicate.setValue(new FilterPredicateValue<>("LoRa-"));
        nameFilter.setPredicate(predicate);
        nameFilter.setValueType(EntityKeyValueType.STRING);

        return new EntityDataQuery(filter, pageLink, entityFields, latestValues, Arrays.asList(nameFilter));
    }

}
