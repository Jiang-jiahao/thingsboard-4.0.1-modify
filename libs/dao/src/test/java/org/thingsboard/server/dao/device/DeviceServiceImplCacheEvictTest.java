package org.thingsboard.server.dao.device;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.thingsboard.server.cache.VersionedTbCache;
import org.thingsboard.server.cache.device.DeviceCacheKey;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.dao.entity.EntityCountService;
import org.thingsboard.server.dao.event.EventService;
import org.thingsboard.server.dao.service.validator.DeviceDataValidator;
import org.thingsboard.server.dao.sql.JpaExecutorService;
import org.thingsboard.server.dao.tenant.TenantService;

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
