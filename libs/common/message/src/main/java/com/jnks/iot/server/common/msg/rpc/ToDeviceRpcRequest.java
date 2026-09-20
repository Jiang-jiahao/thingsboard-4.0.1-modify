package com.jnks.iot.server.common.msg.rpc;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.rpc.ToDeviceRpcRequestBody;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Andrew Shvayka
 */
@Data
public class ToDeviceRpcRequest implements Serializable {

    private static final long serialVersionUID = -7089247105087346214L;

    private final UUID id;
    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final boolean oneway;
    private final long expirationTime;
    private final ToDeviceRpcRequestBody body;
    private final boolean persisted;
    private final Integer retries;
    @JsonIgnore
    private final String additionalInfo;
}
