package com.jnks.iot.rule.engine.flow;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.api.RuleEngineAssetProfileCache;
import com.jnks.iot.rule.engine.api.RuleEngineDeviceProfileCache;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotRuleChainInputNodeTest extends AbstractRuleNodeUpgradeTest {

    private final TenantId TENANT_ID = new TenantId(UUID.fromString("4ba69ea5-6b27-42df-ab66-e7a727a67027"));
    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("97731954-2147-4176-8f1a-d14f1b73e4e6"));
    private final AssetId ASSET_ID = new AssetId(UUID.fromString("841a47bd-4e8e-4ea5-88e6-420da0d70e51"));

    private JnksIotRuleChainInputNode node;
    private JnksIotRuleChainInputNodeConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;

    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private RuleEngineDeviceProfileCache deviceProfileCacheMock;
    @Mock
    private RuleEngineAssetProfileCache assetProfileCacheMock;

    @BeforeEach
    public void setUp() {
        node = spy(new JnksIotRuleChainInputNode());
        config = new JnksIotRuleChainInputNodeConfiguration().defaultConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(config.getRuleChainId()).isNull();
        assertThat(config.isForwardMsgToDefaultRuleChain()).isFalse();
    }

    @ParameterizedTest
    @MethodSource
    public void givenValidConfig_whenInit_thenOk(String ruleChainIdStr, boolean forwardMsgToDefaultRuleChain) throws JnksIotNodeException {
        //GIVEN
        config.setRuleChainId(ruleChainIdStr);
        config.setForwardMsgToDefaultRuleChain(forwardMsgToDefaultRuleChain);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        //WHEN
        assertThatCode(() -> node.init(ctxMock, nodeConfiguration))
                .doesNotThrowAnyException();

        //THEN
        verify(ctxMock).checkTenantEntity(new RuleChainId(UUID.fromString(ruleChainIdStr)));
    }

    private static Stream<Arguments> givenValidConfig_whenInit_thenOk() {
        return Stream.of(
                Arguments.of("45bba7c4-04bf-419b-ae03-6ceb9724f10e", false),
                Arguments.of("52d57e1b-70bb-480e-bcc4-6710e1dcc9d8", true)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"91acbce0-079fdb", "", "  ", "my test string"})
    public void givenInvalidRuleChainId_whenInit_thenThrowsException(String ruleChainIdStr) {
        //GIVEN
        config.setRuleChainId(ruleChainIdStr);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        //WHEN-THEN
        Assertions.assertThatThrownBy(() -> node.init(ctxMock, nodeConfiguration))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("Failed to parse rule chain id: " + ruleChainIdStr);
    }

    @Test
    public void givenRuleChainIdIsNotSet_whenInit_thenThrowsException() {
        assertThatThrownBy(() -> node.init(ctxMock, nodeConfiguration))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("Rule chain must be set!");
    }

    @Test
    public void givenForwardMsgToDefaultIsTrue_whenOnMsg_thenShouldTransferToDeviceDefaultRuleChain() throws JnksIotNodeException {
        //GIVEN
        DeviceProfile deviceProfile = new DeviceProfile();
        RuleChainId defaultRuleChainId = new RuleChainId(UUID.fromString("196e3cd5-68b8-421e-a0cf-1d44fa377cdf"));
        deviceProfile.setDefaultRuleChainId(defaultRuleChainId);

        JnksIotMsg msg = getMsg(DEVICE_ID);

        String ruleChainIdFromConfigStr = "acbc924f-7f95-4a9b-a854-e4822deb74c7";
        config.setRuleChainId(ruleChainIdFromConfigStr);
        config.setForwardMsgToDefaultRuleChain(true);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        when(ctxMock.getDeviceProfileCache()).thenReturn(deviceProfileCacheMock);
        when(deviceProfileCacheMock.get(any(TenantId.class), any(DeviceId.class))).thenReturn(deviceProfile);

        node.init(ctxMock, nodeConfiguration);

        //WHEN
        node.onMsg(ctxMock, msg);

        //THEN
        ArgumentCaptor<RuleChainId> ruleChainArgumentCaptor = ArgumentCaptor.forClass(RuleChainId.class);
        verify(ctxMock).input(eq(msg), ruleChainArgumentCaptor.capture());
        RuleChainId expectedRuleChainId = ruleChainArgumentCaptor.getValue();
        assertThat(expectedRuleChainId).isEqualTo(defaultRuleChainId);

        RuleChainId ruleChainId = (RuleChainId) ReflectionTestUtils.getField(node, "ruleChainId");
        assertThat(ruleChainId).isEqualTo(new RuleChainId(UUID.fromString(ruleChainIdFromConfigStr)));
    }

    @Test
    public void givenForwardMsgToDefaultIsTrue_whenOnMsg_thenShouldTransferToAssetDefaultRuleChain() throws JnksIotNodeException {
        //GIVEN
        AssetProfile assetProfile = new AssetProfile();
        RuleChainId defaultRuleChainId = new RuleChainId(UUID.fromString("f0a3cd58-980c-4730-a40c-8f59064d2065"));
        assetProfile.setDefaultRuleChainId(defaultRuleChainId);

        JnksIotMsg msg = getMsg(ASSET_ID);

        String ruleChainIdFromConfigStr = "56f1c0b8-1a00-4ce0-b3ab-a1416d7cc429";
        config.setRuleChainId(ruleChainIdFromConfigStr);
        config.setForwardMsgToDefaultRuleChain(true);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        when(ctxMock.getAssetProfileCache()).thenReturn(assetProfileCacheMock);
        when(assetProfileCacheMock.get(any(TenantId.class), any(AssetId.class))).thenReturn(assetProfile);

        node.init(ctxMock, nodeConfiguration);

        //WHEN
        node.onMsg(ctxMock, msg);

        //THEN
        ArgumentCaptor<RuleChainId> ruleChainArgumentCaptor = ArgumentCaptor.forClass(RuleChainId.class);
        verify(ctxMock).input(eq(msg), ruleChainArgumentCaptor.capture());
        RuleChainId expectedRuleChainId = ruleChainArgumentCaptor.getValue();
        assertThat(expectedRuleChainId).isEqualTo(defaultRuleChainId);

        RuleChainId ruleChainId = (RuleChainId) ReflectionTestUtils.getField(node, "ruleChainId");
        assertThat(ruleChainId).isEqualTo(new RuleChainId(UUID.fromString(ruleChainIdFromConfigStr)));
    }

    @Test
    public void givenForwardMsgToDefaultIsTrueWithoutDeviceDefaultRuleChain_whenOnMsg_thenShouldTransferToRuleChainFromConfig() throws JnksIotNodeException {
        //GIVEN
        DeviceProfile deviceProfile = new DeviceProfile();

        JnksIotMsg msg = getMsg(DEVICE_ID);

        String ruleChainIdFromConfigStr = "357c2785-e7cc-46a8-9797-957180dabdeb";
        RuleChainId ruleChainIdFromConfig = new RuleChainId(UUID.fromString(ruleChainIdFromConfigStr));
        config.setRuleChainId(ruleChainIdFromConfigStr);
        config.setForwardMsgToDefaultRuleChain(true);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        when(ctxMock.getDeviceProfileCache()).thenReturn(deviceProfileCacheMock);
        when(deviceProfileCacheMock.get(any(TenantId.class), any(DeviceId.class))).thenReturn(deviceProfile);

        node.init(ctxMock, nodeConfiguration);

        //WHEN
        node.onMsg(ctxMock, msg);

        //THEN
        ArgumentCaptor<RuleChainId> ruleChainArgumentCaptor = ArgumentCaptor.forClass(RuleChainId.class);
        verify(ctxMock).input(eq(msg), ruleChainArgumentCaptor.capture());
        assertThat(ruleChainArgumentCaptor.getValue()).isEqualTo(ruleChainIdFromConfig);
    }

    @Test
    public void givenForwardMsgToDefaultIsTrueWithoutAssetDefaultRuleChain_whenOnMsg_thenShouldTransferToRuleChainFromConfig() throws JnksIotNodeException {
        //GIVEN
        AssetProfile assetProfile = new AssetProfile();

        JnksIotMsg msg = getMsg(ASSET_ID);

        String ruleChainIdFromConfigStr = "12883c3d-c10b-4d5b-b606-a59385a920bc";
        RuleChainId ruleChainIdFromConfig = new RuleChainId(UUID.fromString(ruleChainIdFromConfigStr));
        config.setRuleChainId(ruleChainIdFromConfigStr);
        config.setForwardMsgToDefaultRuleChain(true);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        when(ctxMock.getAssetProfileCache()).thenReturn(assetProfileCacheMock);
        when(assetProfileCacheMock.get(any(TenantId.class), any(AssetId.class))).thenReturn(assetProfile);

        node.init(ctxMock, nodeConfiguration);

        //WHEN
        node.onMsg(ctxMock, msg);

        //THEN
        ArgumentCaptor<RuleChainId> ruleChainArgumentCaptor = ArgumentCaptor.forClass(RuleChainId.class);
        verify(ctxMock).input(eq(msg), ruleChainArgumentCaptor.capture());
        assertThat(ruleChainArgumentCaptor.getValue()).isEqualTo(ruleChainIdFromConfig);
    }

    @Test
    public void givenRuleChainInConfig_whenOnMsg_thenShouldTransferToRuleChainFromConfig() throws JnksIotNodeException {
        //GIVEN
        String ruleChainIdFromConfigStr = "3c02c8b3-645c-4e67-aac5-f984f59471d1";
        RuleChainId ruleChainIdFromConfig = new RuleChainId(UUID.fromString(ruleChainIdFromConfigStr));

        JnksIotMsg msg = getMsg(DEVICE_ID);

        config.setRuleChainId(ruleChainIdFromConfigStr);
        config.setForwardMsgToDefaultRuleChain(false);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        node.init(ctxMock, nodeConfiguration);

        //WHEN
        node.onMsg(ctxMock, msg);

        //THEN
        ArgumentCaptor<RuleChainId> ruleChainArgumentCaptor = ArgumentCaptor.forClass(RuleChainId.class);
        verify(ctxMock).input(eq(msg), ruleChainArgumentCaptor.capture());
        assertThat(ruleChainArgumentCaptor.getValue()).isEqualTo(ruleChainIdFromConfig);
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                //config for version 0
                Arguments.of(0,
                        "{\"ruleChainId\": null}",
                        true,
                        "{\"ruleChainId\": null, \"forwardMsgToDefaultRuleChain\": false}"
                ),
                //config for version 1 with upgrade from version 0
                Arguments.of(1,
                        "{\"ruleChainId\": null, \"forwardMsgToDefaultRuleChain\": false}",
                        false,
                        "{\"ruleChainId\": null, \"forwardMsgToDefaultRuleChain\": false}"
                )
        );
    }

    @Override
    protected JnksIotNode getTestNode() {
        return node;
    }

    private JnksIotMsg getMsg(EntityId entityId) {
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(entityId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_STRING)
                .build();
    }
}
