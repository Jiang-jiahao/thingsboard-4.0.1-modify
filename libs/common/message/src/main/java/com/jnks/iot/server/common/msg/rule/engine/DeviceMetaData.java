package com.jnks.iot.server.common.msg.rule.engine;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;

/**
 * Contains basic device metadata;
 *
 * @author ashvayka
 */
@Data
public final class DeviceMetaData {

    final DeviceId deviceId;
    final String deviceName;
    final String deviceType;
    final DeviceAttributes deviceAttributes;

}
