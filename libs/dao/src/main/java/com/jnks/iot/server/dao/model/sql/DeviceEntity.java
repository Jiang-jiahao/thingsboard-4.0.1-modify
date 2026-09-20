package com.jnks.iot.server.dao.model.sql;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.dao.model.ModelConstants;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ModelConstants.DEVICE_TABLE_NAME)
public final class DeviceEntity extends AbstractDeviceEntity<Device> {

    public DeviceEntity() {
        super();
    }

    public DeviceEntity(Device device) {
        super(device);
    }

    @Override
    public Device toData() {
        return super.toDevice();
    }
}
