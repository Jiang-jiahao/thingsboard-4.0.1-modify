package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "device profile switch",
        customRelations = true,
        relationTypes = {"default"},
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "Route incoming messages based on the name of the device profile",
        nodeDetails = "Route incoming messages based on the name of the device profile. The device profile name is case-sensitive<br><br>" +
                "Output connections: <i>Device profile name</i> or <code>Failure</code>",
        configDirective = "jnksIotNodeEmptyConfig")
public class JnksIotDeviceTypeSwitchNode extends JnksIotAbstractTypeSwitchNode {

    @Override
    protected String getRelationType(JnksIotContext ctx, EntityId originator) throws JnksIotNodeException {
        if (!EntityType.DEVICE.equals(originator.getEntityType())) {
            throw new JnksIotNodeException("Unsupported originator type: " + originator.getEntityType().getNormalName() +
                    "! Only " + EntityType.DEVICE.getNormalName() + " type is allowed.");
        }
        DeviceProfile deviceProfile = ctx.getDeviceProfileCache().get(ctx.getTenantId(), (DeviceId) originator);
        if (deviceProfile == null) {
            throw new JnksIotNodeException("Device profile for entity id: " + originator.getId() + " wasn't found!");
        }
        return deviceProfile.getName();
    }

}
