package com.jnks.iot.rule.engine.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.queue.PartitionChangeMsg;

import java.util.concurrent.ExecutionException;

/**
 * Created by ashvayka on 19.01.18.
 */
public interface JnksIotNode {

    void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException;

    void onMsg(JnksIotContext ctx, JnksIotMsg msg) throws ExecutionException, InterruptedException, JnksIotNodeException;

    default void destroy() {
    }

    default void onPartitionChangeMsg(JnksIotContext ctx, PartitionChangeMsg msg) {
    }

    /**
     * Upgrades the configuration from a specific version to the current version specified in the
     * {@link RuleNode} annotation for the instance of {@link JnksIotNode}.
     *
     * @param fromVersion        The version from which the configuration needs to be upgraded.
     * @param oldConfiguration   The old configuration to be upgraded.
     * @return                   A pair consisting of a Boolean flag indicating the success of the upgrade
     *                           and a JsonNode representing the upgraded configuration.
     * @throws JnksIotNodeException   If an error occurs during the upgrade process.
     */
    default JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        return new JnksIotPair<>(false, oldConfiguration);
    }

}
