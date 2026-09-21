package com.jnks.iot.rule.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.util.JnksIotPair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.willCallRealMethod;

public abstract class AbstractRuleNodeUpgradeTest {

    protected abstract JnksIotNode getTestNode();

    @ParameterizedTest
    @MethodSource
    public void givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig(int givenVersion, String givenConfigStr, boolean hasChanges, String expectedConfigStr) throws JnksIotNodeException {
        // GIVEN
        willCallRealMethod().given(getTestNode()).upgrade(anyInt(), any());
        JsonNode givenConfig = JacksonUtil.toJsonNode(givenConfigStr);
        JsonNode expectedConfig = JacksonUtil.toJsonNode(expectedConfigStr);

        // WHEN
        JnksIotPair<Boolean, JsonNode> upgradeResult = getTestNode().upgrade(givenVersion, givenConfig);

        // THEN
        assertThat(upgradeResult.getFirst()).isEqualTo(hasChanges);
        ObjectNode upgradedConfig = (ObjectNode) upgradeResult.getSecond();
        assertThat(upgradedConfig).isEqualTo(expectedConfig);
    }
}
