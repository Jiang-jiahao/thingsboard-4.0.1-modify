package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.timeseries.TimeseriesService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotGetTenantAttributeNodeTest {

    private static final DeviceId DUMMY_DEVICE_ORIGINATOR = new DeviceId(UUID.randomUUID());
    private static final TenantId TENANT_ID = new TenantId(UUID.randomUUID());
    private static final TestDbCallbackExecutor DB_EXECUTOR = new TestDbCallbackExecutor();
    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private AttributesService attributesServiceMock;
    @Mock
    private TimeseriesService timeseriesServiceMock;
    private JnksIotGetTenantAttributeNode node;
    private JnksIotGetEntityDataNodeConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;
    private JnksIotMsg msg;

    @BeforeEach
    public void setUp() {
        node = new JnksIotGetTenantAttributeNode();
        config = new JnksIotGetEntityDataNodeConfiguration().defaultConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
    }

    @Test
    public void givenConfigWithNullFetchTo_whenInit_thenException() {
        // GIVEN
        config.setFetchTo(null);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("FetchTo option can't be null! Allowed values: " + Arrays.toString(JnksIotMsgSource.values()));
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenConfigWithNullDataToFetch_whenInit_thenException() {
        // GIVEN
        config.setDataToFetch(null);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("DataToFetch property has invalid value: null. Only ATTRIBUTES and LATEST_TELEMETRY values supported!");
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenConfigWithUnsupportedDataToFetch_whenInit_thenException() {
        // GIVEN
        config.setDataToFetch(DataToFetch.FIELDS);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("DataToFetch property has invalid value: FIELDS. Only ATTRIBUTES and LATEST_TELEMETRY values supported!");
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenDefaultConfig_whenInit_thenOK() throws JnksIotNodeException {
        // GIVEN

        // WHEN
        node.init(ctxMock, nodeConfiguration);

        // THEN
        assertThat(node.config).isEqualTo(config);
        assertThat(config.getDataMapping()).isEqualTo(Map.of("alarmThreshold", "threshold"));
        assertThat(config.getDataToFetch()).isEqualTo(DataToFetch.ATTRIBUTES);
        assertThat(node.fetchTo).isEqualTo(JnksIotMsgSource.METADATA);
    }

    @Test
    public void givenCustomConfig_whenInit_thenOK() throws JnksIotNodeException {
        // GIVEN
        config.setDataMapping(Map.of(
                "sourceAttr1", "targetKey1",
                "sourceAttr2", "targetKey2",
                "sourceAttr3", "targetKey3"));
        config.setDataToFetch(DataToFetch.LATEST_TELEMETRY);
        config.setFetchTo(JnksIotMsgSource.DATA);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        node.init(ctxMock, nodeConfiguration);

        // THEN
        assertThat(node.config).isEqualTo(config);
        assertThat(config.getDataMapping()).isEqualTo(Map.of(
                "sourceAttr1", "targetKey1",
                "sourceAttr2", "targetKey2",
                "sourceAttr3", "targetKey3"));
        assertThat(config.getDataToFetch()).isEqualTo(DataToFetch.LATEST_TELEMETRY);
        assertThat(node.fetchTo).isEqualTo(JnksIotMsgSource.DATA);
    }

    @Test
    public void givenEmptyAttributesMapping_whenInit_thenException() {
        // GIVEN
        var expectedExceptionMessage = "At least one mapping entry should be specified!";

        config.setDataMapping(Collections.emptyMap());
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo(expectedExceptionMessage);
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenMsgDataIsNotAnJsonObjectAndFetchToData_whenOnMsg_thenException() {
        // GIVEN
        node.fetchTo = JnksIotMsgSource.DATA;
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DUMMY_DEVICE_ORIGINATOR)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_ARRAY)
                .build();

        // WHEN
        var exception = assertThrows(IllegalArgumentException.class, () -> node.onMsg(ctxMock, msg));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("Message body is not an object!");
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenFetchAttributesToData_whenOnMsg_thenShouldFetchAttributesToData() {
        // GIVEN
        var deviceId = new DeviceId(UUID.randomUUID());

        prepareMsgAndConfig(JnksIotMsgSource.DATA, DataToFetch.ATTRIBUTES, deviceId);

        List<AttributeKvEntry> attributesList = List.of(
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey1", "sourceValue1"), 1L),
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey2", "sourceValue2"), 2L),
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey3", "sourceValue3"), 3L)
        );
        var expectedPatternProcessedKeysList = List.of("sourceKey1", "sourceKey2", "sourceKey3");

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getAttributesService()).thenReturn(attributesServiceMock);
        when(attributesServiceMock.find(eq(TENANT_ID), eq(TENANT_ID), eq(AttributeScope.SERVER_SCOPE), argThat(new ListMatcher<>(expectedPatternProcessedKeysList))))
                .thenReturn(Futures.immediateFuture(attributesList));

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgData = "{\"temp\":42," +
                "\"humidity\":77," +
                "\"messageBodyPattern1\":\"targetKey2\"," +
                "\"messageBodyPattern2\":\"sourceKey3\"," +
                "\"targetKey1\":\"sourceValue1\"," +
                "\"targetKey2\":\"sourceValue2\"," +
                "\"targetKey3\":\"sourceValue3\"}";

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(expectedMsgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenFetchAttributesToMetaData_whenOnMsg_thenShouldFetchAttributesToMetaData() {
        // GIVEN
        prepareMsgAndConfig(JnksIotMsgSource.METADATA, DataToFetch.ATTRIBUTES, TENANT_ID);

        List<AttributeKvEntry> attributesList = List.of(
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey1", "sourceValue1"), 1L),
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey2", "sourceValue2"), 2L),
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey3", "sourceValue3"), 3L)
        );
        var expectedPatternProcessedKeysList = List.of("sourceKey1", "sourceKey2", "sourceKey3");

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getAttributesService()).thenReturn(attributesServiceMock);
        when(attributesServiceMock.find(eq(TENANT_ID), eq(TENANT_ID), eq(AttributeScope.SERVER_SCOPE), argThat(new ListMatcher<>(expectedPatternProcessedKeysList))))
                .thenReturn(Futures.immediateFuture(attributesList));

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgMetaData = new JnksIotMsgMetaData(Map.of(
                "metaDataPattern1", "sourceKey2",
                "metaDataPattern2", "targetKey3",
                "targetKey1", "sourceValue1",
                "targetKey2", "sourceValue2",
                "targetKey3", "sourceValue3"
        ));

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(expectedMsgMetaData);
    }

    @Test
    public void givenFetchTelemetryToData_whenOnMsg_thenShouldFetchTelemetryToData() {
        // GIVEN
        var customerId = new CustomerId(UUID.randomUUID());

        prepareMsgAndConfig(JnksIotMsgSource.DATA, DataToFetch.LATEST_TELEMETRY, customerId);

        List<TsKvEntry> timeseries = List.of(
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey1", "sourceValue1")),
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey2", "sourceValue2")),
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey3", "sourceValue3"))
        );
        var expectedPatternProcessedKeysList = List.of("sourceKey1", "sourceKey2", "sourceKey3");

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getTimeseriesService()).thenReturn(timeseriesServiceMock);
        when(timeseriesServiceMock.findLatest(eq(TENANT_ID), eq(TENANT_ID), argThat(new ListMatcher<>(expectedPatternProcessedKeysList))))
                .thenReturn(Futures.immediateFuture(timeseries));

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgData = "{\"temp\":42," +
                "\"humidity\":77," +
                "\"messageBodyPattern1\":\"targetKey2\"," +
                "\"messageBodyPattern2\":\"sourceKey3\"," +
                "\"targetKey1\":\"sourceValue1\"," +
                "\"targetKey2\":\"sourceValue2\"," +
                "\"targetKey3\":\"sourceValue3\"}";

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(expectedMsgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenFetchTelemetryToMetaData_whenOnMsg_thenShouldFetchTelemetryToMetaData() {
        // GIVEN
        var ruleChainId = new RuleChainId(UUID.randomUUID());

        prepareMsgAndConfig(JnksIotMsgSource.METADATA, DataToFetch.LATEST_TELEMETRY, ruleChainId);

        List<TsKvEntry> timeseries = List.of(
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey1", "sourceValue1")),
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey2", "sourceValue2")),
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey3", "sourceValue3"))
        );
        var expectedPatternProcessedKeysList = List.of("sourceKey1", "sourceKey2", "sourceKey3");

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getTimeseriesService()).thenReturn(timeseriesServiceMock);
        when(timeseriesServiceMock.findLatest(eq(TENANT_ID), eq(TENANT_ID), argThat(new ListMatcher<>(expectedPatternProcessedKeysList))))
                .thenReturn(Futures.immediateFuture(timeseries));

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgMetaData = new JnksIotMsgMetaData(Map.of(
                "metaDataPattern1", "sourceKey2",
                "metaDataPattern2", "targetKey3",
                "targetKey1", "sourceValue1",
                "targetKey2", "sourceValue2",
                "targetKey3", "sourceValue3"
        ));

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(expectedMsgMetaData);
    }

    @Test
    public void givenOldConfig_whenUpgrade_thenShouldReturnTrueResultWithNewConfig() throws Exception {
        var defaultConfig = new JnksIotGetEntityDataNodeConfiguration().defaultConfiguration();
        var node = new JnksIotGetTenantAttributeNode();
        String oldConfig = "{\"attrMapping\":{\"alarmThreshold\":\"threshold\"},\"telemetry\":false}";
        JsonNode configJson = JacksonUtil.toJsonNode(oldConfig);
        JnksIotPair<Boolean, JsonNode> upgrade = node.upgrade(0, configJson);
        Assertions.assertTrue(upgrade.getFirst());
        Assertions.assertEquals(defaultConfig, JacksonUtil.treeToValue(upgrade.getSecond(), defaultConfig.getClass()));
    }

    private void prepareMsgAndConfig(JnksIotMsgSource fetchTo, DataToFetch dataToFetch, EntityId originator) {
        config.setDataMapping(Map.of(
                "sourceKey1", "targetKey1",
                "${metaDataPattern1}", "$[messageBodyPattern1]",
                "$[messageBodyPattern2]", "${metaDataPattern2}"));
        config.setDataToFetch(dataToFetch);
        config.setFetchTo(fetchTo);

        node.config = config;
        node.fetchTo = fetchTo;

        var msgMetaData = new JnksIotMsgMetaData();
        msgMetaData.putValue("metaDataPattern1", "sourceKey2");
        msgMetaData.putValue("metaDataPattern2", "targetKey3");

        var msgData = "{\"temp\":42,\"humidity\":77,\"messageBodyPattern1\":\"targetKey2\",\"messageBodyPattern2\":\"sourceKey3\"}";

        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .build();
    }

    @RequiredArgsConstructor
    private static class ListMatcher<T> implements ArgumentMatcher<List<T>> {

        private final List<T> expectedList;

        @Override
        public boolean matches(List<T> actualList) {
            if (actualList == expectedList) {
                return true;
            }
            if (actualList.size() != expectedList.size()) {
                return false;
            }
            return actualList.containsAll(expectedList);
        }

    }

}
