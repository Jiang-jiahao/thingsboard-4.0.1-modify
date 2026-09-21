package com.jnks.iot.rule.engine.metadata;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.ListeningExecutor;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.data.DeviceRelationsQuery;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.device.DeviceSearchQuery;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.device.DeviceService;

import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.spy;
import static org.mockito.BDDMockito.then;

@ExtendWith(MockitoExtension.class)
public class JnksIotGetDeviceAttrNodeTest extends AbstractRuleNodeUpgradeTest {

    private final TenantId TENANT_ID = new TenantId(UUID.fromString("5aea576c-66c4-4732-86b8-dc6bfcde7443"));
    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("40b6b393-6ddf-47f9-973a-18550ca70384"));
    private final ListeningExecutor executor = new TestDbCallbackExecutor();


    private JnksIotGetDeviceAttrNode node;
    private JnksIotGetDeviceAttrNodeConfiguration config;

    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private DeviceService deviceServiceMock;

    @BeforeEach
    public void setUp() {
        node = spy(new JnksIotGetDeviceAttrNode());
        config = new JnksIotGetDeviceAttrNodeConfiguration().defaultConfiguration();
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(config.getClientAttributeNames()).isEmpty();
        assertThat(config.getSharedAttributeNames()).isEmpty();
        assertThat(config.getServerAttributeNames()).isEmpty();
        assertThat(config.getLatestTsKeyNames()).isEmpty();
        assertThat(config.isTellFailureIfAbsent()).isTrue();
        assertThat(config.isGetLatestValueWithTs()).isFalse();
        assertThat(config.getFetchTo()).isEqualTo(JnksIotMsgSource.METADATA);

        var deviceRelationsQuery = new DeviceRelationsQuery();
        deviceRelationsQuery.setDirection(EntitySearchDirection.FROM);
        deviceRelationsQuery.setMaxLevel(1);
        deviceRelationsQuery.setRelationType(EntityRelation.CONTAINS_TYPE);
        deviceRelationsQuery.setDeviceTypes(Collections.singletonList("default"));

        assertThat(config.getDeviceRelationsQuery()).isEqualTo(deviceRelationsQuery);
    }

    @Test
    public void givenFetchToIsNull_whenInit_thenThrowsException() {
        config.setFetchTo(null);
        assertThatThrownBy(() -> node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config))))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("FetchTo option can't be null! Allowed values: " + Arrays.toString(JnksIotMsgSource.values()));
    }

    @Test
    public void givenDeviceDoesNotExist_whenOnMsg_thenTellFailure() throws JnksIotNodeException {
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        given(ctxMock.getDeviceService()).willReturn(deviceServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(deviceServiceMock.findDevicesByQuery(any(TenantId.class), any(DeviceSearchQuery.class))).willReturn(Futures.immediateFuture(Collections.emptyList()));
        given(ctxMock.getDbCallbackExecutor()).willReturn(executor);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<Throwable> actualException = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(eq(msg), actualException.capture());
        assertThat(actualException.getValue())
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("Failed to find related device to message originator using relation query specified in the configuration!");
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                // config for version 1 with upgrade from version 0
                Arguments.of(0,
                        """
                                {
                                "fetchToData":false,
                                "clientAttributeNames":[],
                                "sharedAttributeNames":[],
                                "serverAttributeNames":[],
                                "latestTsKeyNames":[],
                                "tellFailureIfAbsent":true,
                                "getLatestValueWithTs":false,
                                "deviceRelationsQuery":{"direction":"FROM","maxLevel":1,"relationType":"Contains","deviceTypes":["default"],"fetchLastLevelOnly":false}
                                }
                        """,
                        true,
                        """
                                {
                                "deviceRelationsQuery": {"direction": "FROM","maxLevel": 1, "relationType": "Contains","deviceTypes": ["default"],"fetchLastLevelOnly": false},
                                "tellFailureIfAbsent": true,
                                "fetchTo": "METADATA",
                                "clientAttributeNames": [],
                                "sharedAttributeNames": [],
                                "serverAttributeNames": [],
                                "latestTsKeyNames": [],
                                "getLatestValueWithTs": false
                                }
                        """
                ),
                // config for version 1 with upgrade from version 0 (old config with no fetchToData property)
                Arguments.of(0,
                        """
                                {
                                "clientAttributeNames":[],
                                "sharedAttributeNames":[],
                                "serverAttributeNames":[],
                                "latestTsKeyNames":[],
                                "tellFailureIfAbsent":true,
                                "getLatestValueWithTs":false,
                                "deviceRelationsQuery":{"direction":"FROM","maxLevel":1,"relationType":"Contains","deviceTypes":["default"],"fetchLastLevelOnly":false}
                                }
                        """,
                        true,
                        """
                                {
                                "deviceRelationsQuery": {"direction": "FROM","maxLevel": 1, "relationType": "Contains","deviceTypes": ["default"],"fetchLastLevelOnly": false},
                                "tellFailureIfAbsent": true,
                                "fetchTo": "METADATA",
                                "clientAttributeNames": [],
                                "sharedAttributeNames": [],
                                "serverAttributeNames": [],
                                "latestTsKeyNames": [],
                                "getLatestValueWithTs": false
                                }
                        """
                ),
                // config for version 1 with upgrade from version 0 (old config with null fetchToData property)
                Arguments.of(0,
                        """
                                {
                                "fetchToData":null,
                                "clientAttributeNames":[],
                                "sharedAttributeNames":[],
                                "serverAttributeNames":[],
                                "latestTsKeyNames":[],
                                "tellFailureIfAbsent":true,
                                "getLatestValueWithTs":false,
                                "deviceRelationsQuery":{"direction":"FROM","maxLevel":1,"relationType":"Contains","deviceTypes":["default"],"fetchLastLevelOnly":false}
                                }
                        """,
                        true,
                        """
                                {
                                "deviceRelationsQuery": {"direction": "FROM","maxLevel": 1, "relationType": "Contains","deviceTypes": ["default"],"fetchLastLevelOnly": false},
                                "tellFailureIfAbsent": true,
                                "fetchTo": "METADATA",
                                "clientAttributeNames": [],
                                "sharedAttributeNames": [],
                                "serverAttributeNames": [],
                                "latestTsKeyNames": [],
                                "getLatestValueWithTs": false
                                }
                        """
                ),
                // config for version 1 with upgrade from version 1
                Arguments.of(1,
                        """
                                {
                                "deviceRelationsQuery": {"direction": "FROM","maxLevel": 1, "relationType": "Contains","deviceTypes": ["default"],"fetchLastLevelOnly": false},
                                "tellFailureIfAbsent": true,
                                "fetchTo": "METADATA",
                                "clientAttributeNames": [],
                                "sharedAttributeNames": [],
                                "serverAttributeNames": [],
                                "latestTsKeyNames": [],
                                "getLatestValueWithTs": false
                                }
                        """,
                        false,
                        """
                                {
                                "deviceRelationsQuery": {"direction": "FROM","maxLevel": 1, "relationType": "Contains","deviceTypes": ["default"],"fetchLastLevelOnly": false},
                                "tellFailureIfAbsent": true,
                                "fetchTo": "METADATA",
                                "clientAttributeNames": [],
                                "sharedAttributeNames": [],
                                "serverAttributeNames": [],
                                "latestTsKeyNames": [],
                                "getLatestValueWithTs": false
                                }
                        """
                )
        );

    }

    @Override
    protected JnksIotNode getTestNode() {
        return node;
    }

}
