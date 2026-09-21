package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.util.EntitiesRelatedDeviceIdAsyncLoader;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "关联设备属性",
        configClazz = JnksIotGetDeviceAttrNodeConfiguration.class,
        version = 1,
        nodeDescription = "将来源方关联设备的属性和/或最新遥测值添加到消息或消息元数据中",
        nodeDetails = "根据配置的关系查询查找关联设备。 " +
                "如果找到多个关联设备，仅使用第一个设备来扩充消息，其他实体将被丢弃。 " +
                "当你需要从与消息来源方存在关系的设备中获取属性和/或最新遥测值， " +
                "并将其用于后续消息处理时非常有用。<br><br>" +
                "输出连接：<code>Success</code>、<code>Failure</code>。",
        configDirective = "jnksIotEnrichmentNodeDeviceAttributesConfig")
public class JnksIotGetDeviceAttrNode extends JnksIotAbstractGetAttributesNode<JnksIotGetDeviceAttrNodeConfiguration, DeviceId> {

    private static final String RELATED_DEVICE_NOT_FOUND_MESSAGE = "Failed to find related device to message originator using relation query specified in the configuration!";

    @Override
    protected JnksIotGetDeviceAttrNodeConfiguration loadNodeConfiguration(JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        return JnksIotNodeUtils.convert(configuration, JnksIotGetDeviceAttrNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<DeviceId> findEntityIdAsync(JnksIotContext ctx, JnksIotMsg msg) {
        return Futures.transformAsync(
                EntitiesRelatedDeviceIdAsyncLoader.findDeviceAsync(ctx, msg.getOriginator(), config.getDeviceRelationsQuery()),
                checkIfEntityIsPresentOrThrow(RELATED_DEVICE_NOT_FOUND_MESSAGE),
                ctx.getDbCallbackExecutor());
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return fromVersion == 0 ?
                upgradeRuleNodesWithOldPropertyToUseFetchTo(
                        oldConfiguration,
                        "fetchToData",
                        JnksIotMsgSource.DATA.name(),
                        JnksIotMsgSource.METADATA.name()) :
                new JnksIotPair<>(false, oldConfiguration);
    }

}
