package com.jnks.iot.server.common.msg;

import com.jnks.iot.server.common.msg.aware.DeviceAwareMsg;
import com.jnks.iot.server.common.msg.aware.TenantAwareMsg;

import java.io.Serializable;

/**
 * @author Andrew Shvayka
 */
public interface ToDeviceActorNotificationMsg extends TbActorMsg, TenantAwareMsg, DeviceAwareMsg, Serializable {

}
