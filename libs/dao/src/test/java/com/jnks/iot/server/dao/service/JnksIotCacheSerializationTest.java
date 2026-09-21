package com.jnks.iot.server.dao.service;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.server.cache.JnksIotTransactionalCache;
import com.jnks.iot.server.common.data.EntitySubtype;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@DaoSqlTest
public class JnksIotCacheSerializationTest extends AbstractServiceTest {

    @Autowired
    JnksIotTransactionalCache<TenantId, PageData<EntitySubtype>> alarmTypesCache;

    @Test
    public void AlarmTypesSerializationTest() {
        var typesCount = 13;
        TenantId tenantId = new TenantId(UUID.randomUUID());
        List<EntitySubtype> types = new ArrayList<>(typesCount);
        for (int i = 0; i < typesCount; i++) {
            types.add(new EntitySubtype(tenantId, EntityType.ALARM, "alarm_type_" + i));
        }
        PageData<EntitySubtype> alarmTypesPage = new PageData<>(types, 1, typesCount, false);
        alarmTypesCache.put(tenantId, alarmTypesPage);
        PageData<EntitySubtype> foundAlarmTypes = alarmTypesCache.get(tenantId).get();
        Assert.assertEquals(alarmTypesPage, foundAlarmTypes);
    }
}
