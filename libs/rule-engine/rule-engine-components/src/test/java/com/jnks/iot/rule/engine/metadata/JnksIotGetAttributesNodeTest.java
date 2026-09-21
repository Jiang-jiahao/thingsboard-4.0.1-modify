package com.jnks.iot.rule.engine.metadata;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.AbstractListeningExecutor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.JsonDataEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.timeseries.TimeseriesService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class JnksIotGetAttributesNodeTest extends AbstractRuleNodeUpgradeTest {

    private final EntityId ORIGINATOR_ID = new DeviceId(UUID.fromString("965f2975-787a-4f21-87e6-9aa4738186ff"));
    private final TenantId TENANT_ID = TenantId.fromUUID(UUID.fromString("befd3239-79b8-4263-a8d1-95b69f44f798"));
    private AbstractListeningExecutor dbExecutor;

    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private AttributesService attributesServiceMock;
    @Mock
    private TimeseriesService timeseriesServiceMock;

    private List<String> clientAttributes;
    private List<String> serverAttributes;
    private List<String> sharedAttributes;
    private List<String> tsKeys;
    private long ts;

    @Spy
    private JnksIotGetAttributesNode node;

    @BeforeEach
    public void before() throws JnksIotNodeException {
        dbExecutor = new AbstractListeningExecutor() {
            @Override
            protected int getThreadPollSize() {
                return 3;
            }
        };
        dbExecutor.init();

        lenient().when(ctxMock.getAttributesService()).thenReturn(attributesServiceMock);
        lenient().when(ctxMock.getTimeseriesService()).thenReturn(timeseriesServiceMock);
        lenient().when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        lenient().when(ctxMock.getDbCallbackExecutor()).thenReturn(dbExecutor);

        clientAttributes = getAttributeNames("client");
        serverAttributes = getAttributeNames("server");
        sharedAttributes = getAttributeNames("shared");
        tsKeys = List.of("temperature", "humidity", "unknown");
        ts = System.currentTimeMillis();

        lenient().when(attributesServiceMock.find(TENANT_ID, ORIGINATOR_ID, AttributeScope.CLIENT_SCOPE, clientAttributes))
                .thenReturn(Futures.immediateFuture(getListAttributeKvEntry(clientAttributes, ts)));

        lenient().when(attributesServiceMock.find(TENANT_ID, ORIGINATOR_ID, AttributeScope.SERVER_SCOPE, serverAttributes))
                .thenReturn(Futures.immediateFuture(getListAttributeKvEntry(serverAttributes, ts)));

        lenient().when(attributesServiceMock.find(TENANT_ID, ORIGINATOR_ID, AttributeScope.SHARED_SCOPE, sharedAttributes))
                .thenReturn(Futures.immediateFuture(getListAttributeKvEntry(sharedAttributes, ts)));

        lenient().when(timeseriesServiceMock.findLatest(TENANT_ID, ORIGINATOR_ID, tsKeys))
                .thenReturn(Futures.immediateFuture(getListTsKvEntry(tsKeys, ts)));
    }

    @AfterEach
    public void after() {
        dbExecutor.destroy();
    }

    @Test
    public void givenFetchAttributesToMetadata_whenOnMsg_thenShouldTellSuccess() throws Exception {
        // GIVEN
        node = initNode(JnksIotMsgSource.METADATA, false, false);
        var msg = getJnksIotMsg(ORIGINATOR_ID);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var resultMsg = checkMsg(true);

        checkAttributes(resultMsg, JnksIotMsgSource.METADATA, "cs_", clientAttributes);
        checkAttributes(resultMsg, JnksIotMsgSource.METADATA, "ss_", serverAttributes);
        checkAttributes(resultMsg, JnksIotMsgSource.METADATA, "shared_", sharedAttributes);

        checkTs(resultMsg, JnksIotMsgSource.METADATA, false, tsKeys);
    }

    @Test
    public void givenFetchLatestTimeseriesToMetadata_whenOnMsg_thenShouldTellSuccess() throws Exception {
        // GIVEN
        node = initNode(JnksIotMsgSource.METADATA, true, false);
        var msg = getJnksIotMsg(ORIGINATOR_ID);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var resultMsg = checkMsg(true);

        checkAttributes(resultMsg, JnksIotMsgSource.METADATA, "cs_", clientAttributes);
        checkAttributes(resultMsg, JnksIotMsgSource.METADATA, "ss_", serverAttributes);
        checkAttributes(resultMsg, JnksIotMsgSource.METADATA, "shared_", sharedAttributes);

        checkTs(resultMsg, JnksIotMsgSource.METADATA, true, tsKeys);
    }

    @Test
    public void givenFetchAttributesToData_whenOnMsg_thenShouldTellSuccess() throws Exception {
        // GIVEN
        node = initNode(JnksIotMsgSource.DATA, false, false);
        var msg = getJnksIotMsg(ORIGINATOR_ID);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var resultMsg = checkMsg(true);

        checkAttributes(resultMsg, JnksIotMsgSource.DATA, "cs_", clientAttributes);
        checkAttributes(resultMsg, JnksIotMsgSource.DATA, "ss_", serverAttributes);
        checkAttributes(resultMsg, JnksIotMsgSource.DATA, "shared_", sharedAttributes);

        checkTs(resultMsg, JnksIotMsgSource.DATA, false, tsKeys);
    }

    @Test
    public void givenFetchLatestTimeseriesToData_whenOnMsg_thenShouldTellSuccess() throws Exception {
        // GIVEN
        node = initNode(JnksIotMsgSource.DATA, true, false);
        var msg = getJnksIotMsg(ORIGINATOR_ID);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var resultMsg = checkMsg(true);

        checkAttributes(resultMsg, JnksIotMsgSource.DATA, "cs_", clientAttributes);
        checkAttributes(resultMsg, JnksIotMsgSource.DATA, "ss_", serverAttributes);
        checkAttributes(resultMsg, JnksIotMsgSource.DATA, "shared_", sharedAttributes);

        checkTs(resultMsg, JnksIotMsgSource.DATA, true, tsKeys);
    }

    @Test
    public void givenFetchAttributesToMetadata_whenOnMsg_thenShouldTellFailure() throws Exception {
        // GIVEN
        node = initNode(JnksIotMsgSource.METADATA, false, true);
        var msg = getJnksIotMsg(ORIGINATOR_ID);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMsg = checkMsg(false);

        checkAttributes(actualMsg, JnksIotMsgSource.METADATA, "cs_", clientAttributes);
        checkAttributes(actualMsg, JnksIotMsgSource.METADATA, "ss_", serverAttributes);
        checkAttributes(actualMsg, JnksIotMsgSource.METADATA, "shared_", sharedAttributes);

        checkTs(actualMsg, JnksIotMsgSource.METADATA, false, tsKeys);
    }

    @Test
    public void givenFetchLatestTimeseriesToData_whenOnMsg_thenShouldTellFailure() throws Exception {
        // GIVEN
        node = initNode(JnksIotMsgSource.DATA, true, true);
        var msg = getJnksIotMsg(ORIGINATOR_ID);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMsg = checkMsg(false);

        checkAttributes(actualMsg, JnksIotMsgSource.DATA, "cs_", clientAttributes);
        checkAttributes(actualMsg, JnksIotMsgSource.DATA, "ss_", serverAttributes);
        checkAttributes(actualMsg, JnksIotMsgSource.DATA, "shared_", sharedAttributes);

        checkTs(actualMsg, JnksIotMsgSource.DATA, true, tsKeys);
    }

    @Test
    public void givenFetchLatestTimeseriesToDataAndDataIsNotJsonObject_whenOnMsg_thenException() throws Exception {
        // GIVEN
        node = initNode(JnksIotMsgSource.DATA, true, true);
        var msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(ORIGINATOR_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_ARRAY)
                .build();

        // WHEN
        var exception = assertThrows(IllegalArgumentException.class, () -> node.onMsg(ctxMock, msg));

        // THEN
        verify(ctxMock, never()).tellSuccess(any());
        assertThat(exception.getMessage()).isEqualTo("Message body is not an object!");
    }

    private JnksIotMsg checkMsg(boolean checkSuccess) {
        var msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        if (checkSuccess) {
            verify(ctxMock, timeout(5000)).tellSuccess(msgCaptor.capture());
        } else {
            var exceptionCaptor = ArgumentCaptor.forClass(RuntimeException.class);
            verify(ctxMock, never()).tellSuccess(any());
            verify(ctxMock, timeout(5000)).tellFailure(msgCaptor.capture(), exceptionCaptor.capture());
            var exception = exceptionCaptor.getValue();
            assertNotNull(exception);
            assertNotNull(exception.getMessage());
            assertTrue(exception.getMessage().startsWith("The following attribute/telemetry keys is not present in the DB:"));
        }

        var resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getMetaData());
        assertNotNull(resultMsg.getData());
        return resultMsg;
    }

    private void checkAttributes(JnksIotMsg actualMsg, JnksIotMsgSource fetchTo, String prefix, List<String> attributes) {
        var msgData = JacksonUtil.toJsonNode(actualMsg.getData());
        attributes.stream()
                .filter(attribute -> !attribute.equals("unknown"))
                .forEach(attribute -> {
                    String result = null;
                    if (JnksIotMsgSource.DATA.equals(fetchTo)) {
                        result = msgData.get(prefix + attribute).asText();
                    } else if (JnksIotMsgSource.METADATA.equals(fetchTo)) {
                        result = actualMsg.getMetaData().getValue(prefix + attribute);
                    }
                    assertNotNull(result);
                    assertEquals(attribute + "_value", result);
                });
    }

    private void checkTs(JnksIotMsg actualMsg, JnksIotMsgSource fetchTo, boolean getLatestValueWithTs, List<String> tsKeys) {
        var msgData = JacksonUtil.toJsonNode(actualMsg.getData());
        long value = 1L;
        for (var key : tsKeys) {
            if (key.equals("unknown")) {
                continue;
            }
            String actualValue = null;
            String expectedValue;
            if (getLatestValueWithTs) {
                expectedValue = "{\"ts\":" + ts + ",\"value\":{\"data\":" + value + "}}";
            } else {
                expectedValue = "{\"data\":" + value + "}";
            }
            if (JnksIotMsgSource.DATA.equals(fetchTo)) {
                actualValue = JacksonUtil.toString(msgData.get(key));
            } else if (JnksIotMsgSource.METADATA.equals(fetchTo)) {
                actualValue = actualMsg.getMetaData().getValue(key);
            }
            assertNotNull(actualValue);
            assertEquals(expectedValue, actualValue);
            value++;
        }
    }

    private JnksIotGetAttributesNode initNode(JnksIotMsgSource fetchTo, boolean getLatestValueWithTs, boolean isTellFailureIfAbsent) throws JnksIotNodeException {
        var config = new JnksIotGetAttributesNodeConfiguration();
        config.setClientAttributeNames(List.of("client_attr_1", "client_attr_2", "${client_attr_metadata}", "unknown"));
        config.setServerAttributeNames(List.of("server_attr_1", "server_attr_2", "${server_attr_metadata}", "unknown"));
        config.setSharedAttributeNames(List.of("shared_attr_1", "shared_attr_2", "$[shared_attr_data]", "unknown"));
        config.setLatestTsKeyNames(List.of("temperature", "humidity", "unknown"));
        config.setFetchTo(fetchTo);
        config.setGetLatestValueWithTs(getLatestValueWithTs);
        config.setTellFailureIfAbsent(isTellFailureIfAbsent);

        var nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        var node = new JnksIotGetAttributesNode();
        node.init(ctxMock, nodeConfiguration);
        return node;
    }

    private JnksIotMsg getJnksIotMsg(EntityId entityId) {
        var msgData = JacksonUtil.newObjectNode();
        msgData.put("shared_attr_data", "shared_attr_3");

        var msgMetaData = new JnksIotMsgMetaData();
        msgMetaData.putValue("client_attr_metadata", "client_attr_3");
        msgMetaData.putValue("server_attr_metadata", "server_attr_3");

        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(entityId)
                .copyMetaData(msgMetaData)
                .data(JacksonUtil.toString(msgData))
                .build();
    }

    private List<String> getAttributeNames(String prefix) {
        return List.of(prefix + "_attr_1", prefix + "_attr_2", prefix + "_attr_3", "unknown");
    }

    private List<AttributeKvEntry> getListAttributeKvEntry(List<String> attributesList, long ts) {
        return attributesList.stream()
                .filter(attribute -> !attribute.equals("unknown"))
                .map(attribute -> toAttributeKvEntry(ts, attribute))
                .collect(Collectors.toList());
    }

    private BaseAttributeKvEntry toAttributeKvEntry(long ts, String attribute) {
        return new BaseAttributeKvEntry(ts, new StringDataEntry(attribute, attribute + "_value"));
    }

    private List<TsKvEntry> getListTsKvEntry(List<String> keysList, long ts) {
        long value = 1L;
        var kvEntriesList = new ArrayList<TsKvEntry>();
        for (var key : keysList) {
            if (key.equals("unknown")) {
                continue;
            }
            String dataValue = "{\"data\":" + value + "}";
            kvEntriesList.add(new BasicTsKvEntry(ts, new JsonDataEntry(key, dataValue)));
            value++;
        }
        return kvEntriesList;
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
                                "getLatestValueWithTs":false
                                }
                        """,
                        true,
                        """
                                {
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
                                "sharedAttributeNames":[],"serverAttributeNames":[],
                                "latestTsKeyNames":[],
                                "tellFailureIfAbsent":true,
                                "getLatestValueWithTs":false
                                }
                        """,
                        true,
                        """
                                {
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
                                "getLatestValueWithTs":false
                                }
                        """,
                        true,
                        """
                                {
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
