package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.ListeningExecutor;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.device.DeviceService;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotGetOriginatorFieldsNodeTest {

    private static final DeviceId DUMMY_DEVICE_ORIGINATOR = new DeviceId(UUID.randomUUID());
    private static final TenantId DUMMY_TENANT_ID = new TenantId(UUID.randomUUID());
    private static final ListeningExecutor DB_EXECUTOR = new TestDbCallbackExecutor();
    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private DeviceService deviceServiceMock;
    private JnksIotGetOriginatorFieldsNode node;
    private JnksIotGetOriginatorFieldsConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;
    private JnksIotMsg msg;

    @BeforeEach
    public void setUp() {
        node = new JnksIotGetOriginatorFieldsNode();
        config = new JnksIotGetOriginatorFieldsConfiguration().defaultConfiguration();
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
    public void givenDefaultConfig_whenInit_thenOK() throws JnksIotNodeException {
        // GIVEN-WHEN
        node.init(ctxMock, nodeConfiguration);

        // THEN
        assertThat(node.config).isEqualTo(config);
        assertThat(config.getDataMapping()).isEqualTo(Map.of(
                "name", "originatorName",
                "type", "originatorType"));
        assertThat(config.isIgnoreNullStrings()).isEqualTo(false);
        assertThat(config.getFetchTo()).isEqualTo(JnksIotMsgSource.METADATA);
        assertThat(node.fetchTo).isEqualTo(JnksIotMsgSource.METADATA);
    }

    @Test
    public void givenCustomConfig_whenInit_thenOK() throws JnksIotNodeException {
        // GIVEN
        config.setDataMapping(Map.of(
                "email", "originatorEmail",
                "title", "originatorTitle",
                "country", "originatorCountry"));
        config.setIgnoreNullStrings(true);
        config.setFetchTo(JnksIotMsgSource.DATA);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        node.init(ctxMock, nodeConfiguration);

        // THEN
        assertThat(node.config).isEqualTo(config);
        assertThat(config.getDataMapping()).isEqualTo(Map.of(
                "email", "originatorEmail",
                "title", "originatorTitle",
                "country", "originatorCountry"));
        assertThat(config.isIgnoreNullStrings()).isEqualTo(true);
        assertThat(config.getFetchTo()).isEqualTo(JnksIotMsgSource.DATA);
        assertThat(node.fetchTo).isEqualTo(JnksIotMsgSource.DATA);
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
    public void givenValidMsgAndFetchToData_whenOnMsg_thenShouldTellSuccessAndFetchToData() throws JnksIotNodeException, ExecutionException, InterruptedException {
        // GIVEN
        var device = new Device();
        device.setId(DUMMY_DEVICE_ORIGINATOR);
        device.setName("Test device");
        device.setType("Test device type");

        config.setDataMapping(Map.of(
                "name", "originatorName",
                "type", "originatorType",
                "label", "originatorLabel"));
        config.setIgnoreNullStrings(true);
        config.setFetchTo(JnksIotMsgSource.DATA);

        node.config = config;
        node.fetchTo = JnksIotMsgSource.DATA;
        var msgMetaData = new JnksIotMsgMetaData();
        var msgData = "{\"temp\":42,\"humidity\":77}";
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DUMMY_DEVICE_ORIGINATOR)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .build();

        when(ctxMock.getDeviceService()).thenReturn(deviceServiceMock);
        when(ctxMock.getTenantId()).thenReturn(DUMMY_TENANT_ID);
        when(deviceServiceMock.findDeviceById(eq(DUMMY_TENANT_ID), eq(device.getId()))).thenReturn(device);

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgData = "{\"temp\":42,\"humidity\":77,\"originatorName\":\"Test device\",\"originatorType\":\"Test device type\"}";

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(expectedMsgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msgMetaData);
    }

    @Test
    public void givenDeviceWithEmptyLabel_whenOnMsg_thenShouldTellSuccessAndFetchToData() throws JnksIotNodeException, ExecutionException, InterruptedException {
        // GIVEN
        var device = new Device();
        device.setId(DUMMY_DEVICE_ORIGINATOR);
        device.setName("Test device");
        device.setType("Test device type");
        device.setLabel("");

        config.setDataMapping(Map.of(
                "name", "originatorName",
                "type", "originatorType",
                "label", "originatorLabel"));
        config.setIgnoreNullStrings(true);
        config.setFetchTo(JnksIotMsgSource.DATA);

        node.config = config;
        node.fetchTo = JnksIotMsgSource.DATA;
        var msgMetaData = new JnksIotMsgMetaData();
        var msgData = "{\"temp\":42,\"humidity\":77}";
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DUMMY_DEVICE_ORIGINATOR)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .build();

        when(ctxMock.getDeviceService()).thenReturn(deviceServiceMock);
        when(ctxMock.getTenantId()).thenReturn(DUMMY_TENANT_ID);
        when(deviceServiceMock.findDeviceById(eq(DUMMY_TENANT_ID), eq(device.getId()))).thenReturn(device);

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgData = "{\"temp\":42,\"humidity\":77,\"originatorName\":\"Test device\",\"originatorType\":\"Test device type\"}";

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(expectedMsgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msgMetaData);
    }

    @Test
    public void givenValidMsgAndFetchToMetaData_whenOnMsg_thenShouldTellSuccessAndFetchToMetaData() throws JnksIotNodeException, ExecutionException, InterruptedException {
        // GIVEN
        var device = new Device();
        device.setId(DUMMY_DEVICE_ORIGINATOR);
        device.setName("Test device");
        device.setType("Test device type");

        config.setDataMapping(Map.of(
                "name", "originatorName",
                "type", "originatorType",
                "label", "originatorLabel"));
        config.setIgnoreNullStrings(true);
        config.setFetchTo(JnksIotMsgSource.METADATA);

        node.config = config;
        node.fetchTo = JnksIotMsgSource.METADATA;
        var msgMetaData = new JnksIotMsgMetaData(Map.of(
                "testKey1", "testValue1",
                "testKey2", "123"));
        var msgData = "[\"value1\",\"value2\"]";
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DUMMY_DEVICE_ORIGINATOR)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .build();

        when(ctxMock.getDeviceService()).thenReturn(deviceServiceMock);
        when(ctxMock.getTenantId()).thenReturn(DUMMY_TENANT_ID);
        when(deviceServiceMock.findDeviceById(eq(DUMMY_TENANT_ID), eq(device.getId()))).thenReturn(device);

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgMetaData = new JnksIotMsgMetaData(Map.of(
                "testKey1", "testValue1",
                "testKey2", "123",
                "originatorName", "Test device",
                "originatorType", "Test device type"
        ));

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(expectedMsgMetaData);
    }

    @Test
    public void givenNullEntityFieldsAndIgnoreNullStringsFalse_whenOnMsg_thenShouldTellSuccessAndFetchNullField() throws JnksIotNodeException, ExecutionException, InterruptedException {
        // GIVEN
        var device = new Device();
        device.setId(DUMMY_DEVICE_ORIGINATOR);
        device.setName("Test device");
        device.setType("Test device type");

        config.setDataMapping(Map.of(
                "name", "originatorName",
                "type", "originatorType",
                "label", "originatorLabel"));
        config.setIgnoreNullStrings(false);
        config.setFetchTo(JnksIotMsgSource.METADATA);

        node.config = config;
        node.fetchTo = JnksIotMsgSource.METADATA;
        var msgMetaData = new JnksIotMsgMetaData(Map.of(
                "testKey1", "testValue1",
                "testKey2", "123"));
        var msgData = "[\"value1\",\"value2\"]";
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DUMMY_DEVICE_ORIGINATOR)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .build();

        when(ctxMock.getDeviceService()).thenReturn(deviceServiceMock);
        when(ctxMock.getTenantId()).thenReturn(DUMMY_TENANT_ID);
        when(deviceServiceMock.findDeviceById(eq(DUMMY_TENANT_ID), eq(device.getId()))).thenReturn(device);

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgMetaData = new JnksIotMsgMetaData(Map.of(
                "testKey1", "testValue1",
                "testKey2", "123",
                "originatorName", "Test device",
                "originatorType", "Test device type",
                "originatorLabel", "null"
        ));

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(expectedMsgMetaData);
    }

    @Test
    public void givenEmptyFieldsMapping_whenInit_thenException() {
        // GIVEN
        config.setDataMapping(Collections.emptyMap());
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("At least one mapping entry should be specified!");
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenUnsupportedEntityType_whenOnMsg_thenShouldTellFailureWithSameMsg() throws JnksIotNodeException, ExecutionException, InterruptedException {
        // GIVEN
        config.setDataMapping(Map.of(
                "name", "originatorName",
                "type", "originatorType",
                "label", "originatorLabel"));
        config.setIgnoreNullStrings(false);
        config.setFetchTo(JnksIotMsgSource.METADATA);

        node.config = config;
        node.fetchTo = JnksIotMsgSource.METADATA;
        var msgMetaData = new JnksIotMsgMetaData(Map.of(
                "testKey1", "testValue1",
                "testKey2", "123"));
        var msgData = "[\"value1\",\"value2\"]";
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(new DashboardId(UUID.randomUUID()))
                .copyMetaData(msgMetaData)
                .data(msgData)
                .build();

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctxMock, times(1)).tellFailure(actualMessageCaptor.capture(), any());
        verify(ctxMock, never()).tellSuccess(any());

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msgMetaData);
    }

    @Test
    public void givenOldConfig_whenUpgrade_thenShouldReturnTrueResultWithNewConfig() throws Exception {
        var defaultConfig = new JnksIotGetOriginatorFieldsConfiguration().defaultConfiguration();
        var node = new JnksIotGetOriginatorFieldsNode();
        String oldConfig = "{\"fieldsMapping\":{\"name\":\"originatorName\",\"type\":\"originatorType\"},\"ignoreNullStrings\":false}";
        JsonNode configJson = JacksonUtil.toJsonNode(oldConfig);
        JnksIotPair<Boolean, JsonNode> upgrade = node.upgrade(0, configJson);
        Assertions.assertTrue(upgrade.getFirst());
        Assertions.assertEquals(defaultConfig, JacksonUtil.treeToValue(upgrade.getSecond(), defaultConfig.getClass()));
    }

}
