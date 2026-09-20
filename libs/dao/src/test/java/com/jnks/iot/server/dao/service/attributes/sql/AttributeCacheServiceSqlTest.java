package com.jnks.iot.server.dao.service.attributes.sql;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.server.cache.TbCacheValueWrapper;
import com.jnks.iot.server.cache.VersionedTbCache;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.dao.attributes.AttributeCacheKey;
import com.jnks.iot.server.dao.service.AbstractServiceTest;
import com.jnks.iot.server.dao.service.DaoSqlTest;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@DaoSqlTest
public class AttributeCacheServiceSqlTest extends AbstractServiceTest {

    private static final String TEST_KEY = "key";
    private static final String TEST_VALUE = "value";
    private static final DeviceId DEVICE_ID = new DeviceId(UUID.randomUUID());

    @Autowired
    VersionedTbCache<AttributeCacheKey, AttributeKvEntry> cache;

    @Test
    public void testPutAndGet() {
        AttributeCacheKey testKey = new AttributeCacheKey(AttributeScope.CLIENT_SCOPE, DEVICE_ID, TEST_KEY);
        AttributeKvEntry testValue = new BaseAttributeKvEntry(new StringDataEntry(TEST_KEY, TEST_VALUE), 1, 1L);
        cache.put(testKey, testValue);

        TbCacheValueWrapper<AttributeKvEntry> wrapper = cache.get(testKey);
        assertNotNull(wrapper);

        assertEquals(testValue, wrapper.get());

        AttributeKvEntry testValue2 = new BaseAttributeKvEntry(new StringDataEntry(TEST_KEY, TEST_VALUE), 1, 2L);
        cache.put(testKey, testValue2);

        wrapper = cache.get(testKey);
        assertNotNull(wrapper);

        assertEquals(testValue2, wrapper.get());

        AttributeKvEntry testValue3 = new BaseAttributeKvEntry(new StringDataEntry(TEST_KEY, TEST_VALUE), 1, 0L);
        cache.put(testKey, testValue3);

        wrapper = cache.get(testKey);
        assertNotNull(wrapper);

        assertEquals(testValue2, wrapper.get());

        cache.evict(testKey);
    }

    @Test
    public void testEvictWithVersion() {
        AttributeCacheKey testKey = new AttributeCacheKey(AttributeScope.CLIENT_SCOPE, DEVICE_ID, TEST_KEY);
        AttributeKvEntry testValue = new BaseAttributeKvEntry(new StringDataEntry(TEST_KEY, TEST_VALUE), 1, 1L);
        cache.put(testKey, testValue);

        TbCacheValueWrapper<AttributeKvEntry> wrapper = cache.get(testKey);
        assertNotNull(wrapper);

        assertEquals(testValue, wrapper.get());

        cache.evict(testKey, 2L);

        wrapper = cache.get(testKey);
        assertNotNull(wrapper);

        assertNull(wrapper.get());

        cache.evict(testKey);
    }

    @Test
    public void testEvict() {
        AttributeCacheKey testKey = new AttributeCacheKey(AttributeScope.CLIENT_SCOPE, DEVICE_ID, TEST_KEY);
        AttributeKvEntry testValue = new BaseAttributeKvEntry(new StringDataEntry(TEST_KEY, TEST_VALUE), 1, 1L);
        cache.put(testKey, testValue);

        TbCacheValueWrapper<AttributeKvEntry> wrapper = cache.get(testKey);
        assertNotNull(wrapper);

        assertEquals(testValue, wrapper.get());

        cache.evict(testKey);

        wrapper = cache.get(testKey);
        assertNull(wrapper);
    }
}
