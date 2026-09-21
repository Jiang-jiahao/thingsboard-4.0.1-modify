package com.jnks.iot.server.utils;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.server.common.data.rule.RuleNode;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.service.component.RuleNodeClassInfo;

import static com.jnks.iot.server.common.data.DataConstants.QUEUE_NAME;

@Slf4j
public class JnksIotNodeUpgradeUtils {

    public static void upgradeConfigurationAndVersion(RuleNode node, RuleNodeClassInfo nodeInfo) {
        JsonNode oldConfiguration = node.getConfiguration();
        int configurationVersion = node.getConfigurationVersion();

        int currentVersion = nodeInfo.getCurrentVersion();
        var configClass = nodeInfo.getAnnotation().configClazz();

        if (oldConfiguration == null || !oldConfiguration.isObject()) {
            log.warn("Failed to upgrade rule node with id: {} type: {} fromVersion: {} toVersion: {}. " +
                            "Current configuration is null or not a json object. " +
                            "Going to set default configuration ... ",
                    node.getId(), node.getType(), configurationVersion, currentVersion);
            node.setConfiguration(getDefaultConfig(configClass));
        } else {
            var jnksIotVersionedNode = getJnksIotVersionedNode(nodeInfo);
            try {
                JsonNode queueName = oldConfiguration.get(QUEUE_NAME);
                JnksIotPair<Boolean, JsonNode> upgradeResult = jnksIotVersionedNode.upgrade(configurationVersion, oldConfiguration);
                if (upgradeResult.getFirst()) {
                    node.setConfiguration(upgradeResult.getSecond());
                    if (nodeInfo.getAnnotation().hasQueueName() && queueName != null && queueName.isTextual()) {
                        node.setQueueName(queueName.asText());
                    }
                }
            } catch (Exception e) {
                try {
                    JacksonUtil.treeToValue(oldConfiguration, configClass);
                } catch (Exception ex) {
                    log.warn("Failed to upgrade rule node with id: {} type: {} fromVersion: {} toVersion: {}. " +
                                    "Going to set default configuration ... ",
                            node.getId(), node.getType(), configurationVersion, currentVersion, e);
                    node.setConfiguration(getDefaultConfig(configClass));
                }
            }
        }
        node.setConfigurationVersion(currentVersion);
    }

    @SneakyThrows
    private static JnksIotNode getJnksIotVersionedNode(RuleNodeClassInfo nodeInfo) {
        return (JnksIotNode) nodeInfo.getClazz().getDeclaredConstructor().newInstance();
    }

    @SneakyThrows
    private static JsonNode getDefaultConfig(Class<? extends NodeConfiguration> configClass) {
        return JacksonUtil.valueToTree(configClass.getDeclaredConstructor().newInstance().defaultConfiguration());
    }

}
