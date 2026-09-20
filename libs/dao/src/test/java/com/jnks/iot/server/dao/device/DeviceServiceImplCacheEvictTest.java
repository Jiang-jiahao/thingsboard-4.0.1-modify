package com.jnks.iot.server.dao.device;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import com.jnks.iot.server.cache.VersionedTbCache;
import com.jnks.iot.server.cache.device.DeviceCacheKey;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.entity.EntityCountService;
import com.jnks.iot.server.dao.event.EventService;
import com.jnks.iot.server.dao.service.validator.DeviceDataValidator;
import com.jnks.iot.server.dao.sql.JpaExecutorService;
import com.jnks.iot.server.dao.tenant.TenantService;

import java.util.Collection;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DeviceServiceImplCacheEvictTest {

    @InjectMocks
    private DeviceServiceImpl deviceService;

    @Mock
    private DeviceDao deviceDao;
    @Mock
    private DeviceCredentialsService deviceCredentialsService;
    @Mock
    private DeviceProfileService deviceProfileService;
    @Mock
    private EventService eventService;
    @Mock
    private TenantService tenantService;
    @Mock
    private DeviceDataValidator deviceValidator;
    @Mock
    private EntityCountService countService;
    @Mock
    private JpaExecutorService executor;
    @Mock
    private VersionedTbCache<DeviceCacheKey, Device> cache;

    @BeforeEach
    void injectVersionedCache() {
        ReflectionTestUtils.setField(deviceService, "cache", cache);
    }

    @Test
    public void evictCacheRemovesIdAndNameKeysWithoutPutting() {
        TenantId tenantId = TenantId.fromUUID(UUID.fromString("efe961b0-6b84-11f0-90c5-55df190c490a"));
        DeviceId deviceId = new DeviceId(UUID.fromString("875eb610-ab5f-11f1-a6db-ddefc5643bf3"));

        deviceService.evictCache(tenantId, deviceId, "new-name", "old-name");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<DeviceCacheKey>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(cache).evict(captor.capture());
        assertThat(captor.getValue()).containsExactlyInAnyOrder(
                new DeviceCacheKey(tenantId, "new-name"),
                new DeviceCacheKey(tenantId, "old-name"),
                new DeviceCacheKey(deviceId),
                new DeviceCacheKey(tenantId, deviceId)
        );
        verify(cache, never()).put(any(), any());
    }
}
