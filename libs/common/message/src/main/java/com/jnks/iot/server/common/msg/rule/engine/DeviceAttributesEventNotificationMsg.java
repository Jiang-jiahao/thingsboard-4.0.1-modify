package com.jnks.iot.server.common.msg.rule.engine;

import lombok.Data;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKey;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.ToDeviceActorNotificationMsg;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Andrew Shvayka
 */
@Data
public class DeviceAttributesEventNotificationMsg implements ToDeviceActorNotificationMsg {

    private static final long serialVersionUID = 2422071590415277039L;

    private final TenantId tenantId;
    private final DeviceId deviceId;
    private final Set<AttributeKey> deletedKeys;
    private final String scope;
    private final List<AttributeKvEntry> values;
    private final boolean deleted;

    public static DeviceAttributesEventNotificationMsg onUpdate(TenantId tenantId, DeviceId deviceId, String scope, List<AttributeKvEntry> values) {
        return new DeviceAttributesEventNotificationMsg(tenantId, deviceId, null, scope, values, false);
    }

    public static DeviceAttributesEventNotificationMsg onDelete(TenantId tenantId, DeviceId deviceId, String scope, List<String> keys) {
        Set<AttributeKey> keysToNotify = new HashSet<>();
        keys.forEach(key -> keysToNotify.add(new AttributeKey(scope, key)));
        return new DeviceAttributesEventNotificationMsg(tenantId, deviceId, keysToNotify, null, null, true);
    }

    @Override
    public MsgType getMsgType() {
        return MsgType.DEVICE_ATTRIBUTES_UPDATE_TO_DEVICE_ACTOR_MSG;
    }
}
