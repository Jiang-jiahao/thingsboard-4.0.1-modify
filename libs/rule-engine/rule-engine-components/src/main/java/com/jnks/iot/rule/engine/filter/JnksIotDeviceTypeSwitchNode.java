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
        name = "设备档案切换",
        customRelations = true,
        relationTypes = {"default"},
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "根据设备档案名称路由传入消息",
        nodeDetails = "根据设备档案名称路由传入消息。设备档案名称区分大小写。<br><br>" +
                "输出连接：<i>设备档案名称</i> 或 <code>Failure</code>",
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
