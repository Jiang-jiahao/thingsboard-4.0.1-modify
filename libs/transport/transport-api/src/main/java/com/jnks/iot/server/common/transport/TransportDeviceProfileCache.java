package com.jnks.iot.server.common.transport;

import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.gen.transport.TransportProtos;

public interface TransportDeviceProfileCache {

    DeviceProfile getOrCreate(DeviceProfileId id, TransportProtos.DeviceProfileProto proto);

    DeviceProfile get(DeviceProfileId id);

    void put(DeviceProfile profile);

    DeviceProfile put(TransportProtos.DeviceProfileProto proto);

    void evict(DeviceProfileId id);

}
