package com.jnks.iot.rule.engine.filter;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.plugin.ComponentType;

@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "资产档案分支",
        customRelations = true,
        relationTypes = {"default"},
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "根据资产档案的名称路由传入消息",
        nodeDetails = "根据资产档案的名称路由传入消息。资产档案名称区分大小写。<br><br>" +
                "输出连接：<i>资产档案名称</i>或 <code>Failure</code>",
        configDirective = "jnksIotNodeEmptyConfig")
public class JnksIotAssetTypeSwitchNode extends JnksIotAbstractTypeSwitchNode {

    @Override
    protected String getRelationType(JnksIotContext ctx, EntityId originator) throws JnksIotNodeException {
        if (!EntityType.ASSET.equals(originator.getEntityType())) {
            throw new JnksIotNodeException("Unsupported originator type: " + originator.getEntityType().getNormalName() + "!" +
                    " Only " + EntityType.ASSET.getNormalName() + " type is allowed.");
        }
        AssetProfile assetProfile = ctx.getAssetProfileCache().get(ctx.getTenantId(), (AssetId) originator);
        if (assetProfile == null) {
            throw new JnksIotNodeException("Asset profile for entity id: " + originator.getId() + " wasn't found!");
        }
        return assetProfile.getName();
    }

}
