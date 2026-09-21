package com.jnks.iot.rule.engine.rpc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleEngineDeviceRpcRequest;
import com.jnks.iot.rule.engine.api.RuleEngineDeviceRpcResponse;
import com.jnks.iot.rule.engine.api.RuleEngineRpcService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.rpc.RpcError;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class JnksIotSendRPCRequestNodeTest {

    private final TenantId TENANT_ID = TenantId.fromUUID(UUID.fromString("d3a47f8b-d863-4c1f-b6f0-2c946b43f21c"));
    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("b052ae59-b9b4-47e8-ac71-39e7124bbd66"));

    private final String MSG_DATA = """
            {
              "method": "setGpio",
              "params": {
                "pin": "23",
                "value": 1
              },
              "additionalInfo": "information"
            }
            """;

    private JnksIotSendRPCRequestNode node;
    private JnksIotSendRpcRequestNodeConfiguration config;

    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private RuleEngineRpcService rpcServiceMock;

    @BeforeEach
    public void setUp() throws JnksIotNodeException {
        node = new JnksIotSendRPCRequestNode();
        config = new JnksIotSendRpcRequestNodeConfiguration().defaultConfiguration();
        var configuration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctxMock, configuration);
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(config.getTimeoutInSeconds()).isEqualTo(60);
    }

    @ParameterizedTest
    @MethodSource
    public void givenOneway_whenOnMsg_thenVerifyRequest(String mdKeyValue, boolean expectedResult) {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsgMetaData msgMetadata = new JnksIotMsgMetaData();
        msgMetadata.putValue("oneway", mdKeyValue);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(msgMetadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        var ruleEngineDeviceRpcRequestCaptor = captureRequest();
        assertThat(ruleEngineDeviceRpcRequestCaptor.getValue().isOneway()).isEqualTo(expectedResult);
    }

    private static Stream<Arguments> givenOneway_whenOnMsg_thenVerifyRequest() {
        return Stream.of(
                Arguments.of("true", true),
                Arguments.of("false", false),
                Arguments.of(null, false),
                Arguments.of("", false)
        );
    }

    @Test
    public void givenMsgBody_whenOnMsg_thenVerifyRequest() {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = ArgumentCaptor.forClass(RuleEngineDeviceRpcRequest.class);
        then(rpcServiceMock).should().sendRpcRequestToDevice(requestCaptor.capture(), any(Consumer.class));
        assertThat(requestCaptor.getValue())
                .hasFieldOrPropertyWithValue("method", "setGpio")
                .hasFieldOrPropertyWithValue("body", "{\"pin\":\"23\",\"value\":1}")
                .hasFieldOrPropertyWithValue("deviceId", DEVICE_ID)
                .hasFieldOrPropertyWithValue("tenantId", TENANT_ID)
                .hasFieldOrPropertyWithValue("additionalInfo", "information");
    }

    @Test
    public void givenRequestIdIsNotSet_whenOnMsg_thenVerifyRequest() {
        Random randomMock = mock(Random.class);
        given(randomMock.nextInt()).willReturn(123);
        ReflectionTestUtils.setField(node, "random", randomMock);
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.TO_SERVER_RPC_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getRequestId()).isEqualTo(123);
    }

    @Test
    public void givenRequestId_whenOnMsg_thenVerifyRequest() {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        String data = """
                {
                  "method": "setGpio",
                  "params": {
                    "pin": "23",
                    "value": 1
                  },
                  "requestId": 12345
                }
                """;
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.TO_SERVER_RPC_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(data)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getRequestId()).isEqualTo(12345);
    }

    @Test
    public void givenRequestUUID_whenOnMsg_thenVerifyRequest() {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        String requestUUID = "b795a241-5a30-48fb-92d5-46b864d47130";
        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue("requestUUID", requestUUID);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getRequestUUID()).isEqualTo(UUID.fromString(requestUUID));
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void givenInvalidRequestUUID_whenOnMsg_thenVerifyRequest(String requestUUID) {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue("requestUUID", requestUUID);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getRequestUUID()).isNotNull();
    }

    @Test
    public void givenOriginServiceId_whenOnMsg_thenVerifyRequest() {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        String originServiceId = "service-id-123";
        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue("originServiceId", originServiceId);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getOriginServiceId()).isEqualTo(originServiceId);
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void givenInvalidOriginServiceId_whenOnMsg_thenVerifyRequest(String originServiceId) {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue("originServiceId", originServiceId);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getOriginServiceId()).isNull();
    }

    @Test
    public void givenExpirationTime_whenOnMsg_thenVerifyRequest() {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        String expirationTime = "2000000000000";
        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue(DataConstants.EXPIRATION_TIME, expirationTime);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getExpirationTime()).isEqualTo(Long.parseLong(expirationTime));
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void givenInvalidExpirationTime_whenOnMsg_thenVerifyRequest(String expirationTime) {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue(DataConstants.EXPIRATION_TIME, expirationTime);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getExpirationTime()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    public void givenRetries_whenOnMsg_thenVerifyRequest() {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        Integer retries = 3;
        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue(DataConstants.RETRIES, String.valueOf(retries));
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getRetries()).isEqualTo(retries);
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void givenInvalidRetriesValue_whenOnMsg_thenVerifyRequest(String retries) {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue(DataConstants.RETRIES, retries);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().getRetries()).isNull();
    }

    @ParameterizedTest
    @EnumSource(JnksIotMsgType.class)
    public void givenJnksIotMsgType_whenOnMsg_thenVerifyRequest(JnksIotMsgType msgType) {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(msgType)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        if (msgType == JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE) {
            assertThat(requestCaptor.getValue().isRestApiCall()).isTrue();
            return;
        }
        assertThat(requestCaptor.getValue().isRestApiCall()).isFalse();
    }

    @ParameterizedTest
    @MethodSource
    public void givenPersistent_whenOnMsg_thenVerifyRequest(String isPersisted, boolean expectedPersistence) {
        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);

        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue(DataConstants.PERSISTENT, isPersisted);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = captureRequest();
        assertThat(requestCaptor.getValue().isPersisted()).isEqualTo(expectedPersistence);
    }

    private static Stream<Arguments> givenPersistent_whenOnMsg_thenVerifyRequest() {
        return Stream.of(
                Arguments.of("true", true),
                Arguments.of("false", false),
                Arguments.of(null, false),
                Arguments.of("", false)
        );
    }

    private ArgumentCaptor<RuleEngineDeviceRpcRequest> captureRequest() {
        ArgumentCaptor<RuleEngineDeviceRpcRequest> requestCaptor = ArgumentCaptor.forClass(RuleEngineDeviceRpcRequest.class);
        then(rpcServiceMock).should().sendRpcRequestToDevice(requestCaptor.capture(), any(Consumer.class));
        return requestCaptor;
    }

    @Test
    public void givenRpcResponseWithoutError_whenOnMsg_thenSendsRpcRequest() {
        JnksIotMsg outMsg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();

        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        // TODO: replace deprecated method newMsg()
        given(ctxMock.newMsg(any(), any(String.class), any(), any(), any(), any())).willReturn(outMsg);
        willAnswer(invocation -> {
            Consumer<RuleEngineDeviceRpcResponse> consumer = invocation.getArgument(1);
            RuleEngineDeviceRpcResponse rpcResponseMock = mock(RuleEngineDeviceRpcResponse.class);
            given(rpcResponseMock.getError()).willReturn(Optional.empty());
            given(rpcResponseMock.getResponse()).willReturn(Optional.of(JnksIotMsg.EMPTY_JSON_OBJECT));
            consumer.accept(rpcResponseMock);
            return null;
        }).given(rpcServiceMock).sendRpcRequestToDevice(any(RuleEngineDeviceRpcRequest.class), any(Consumer.class));

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        then(ctxMock).should().enqueueForTellNext(outMsg, JnksIotNodeConnectionType.SUCCESS);
        then(ctxMock).should().ack(msg);
    }

    @Test
    public void givenRpcResponseWithError_whenOnMsg_thenTellFailure() {
        JnksIotMsg outMsg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();

        given(ctxMock.getRpcService()).willReturn(rpcServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        // TODO: replace deprecated method newMsg()
        given(ctxMock.newMsg(any(), any(String.class), any(), any(), any(), any())).willReturn(outMsg);
        willAnswer(invocation -> {
            Consumer<RuleEngineDeviceRpcResponse> consumer = invocation.getArgument(1);
            RuleEngineDeviceRpcResponse rpcResponseMock = mock(RuleEngineDeviceRpcResponse.class);
            given(rpcResponseMock.getError()).willReturn(Optional.of(RpcError.NO_ACTIVE_CONNECTION));
            consumer.accept(rpcResponseMock);
            return null;
        }).given(rpcServiceMock).sendRpcRequestToDevice(any(RuleEngineDeviceRpcRequest.class), any(Consumer.class));

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.RPC_CALL_FROM_SERVER_TO_DEVICE)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(MSG_DATA)
                .build();
        node.onMsg(ctxMock, msg);

        then(ctxMock).should().enqueueForTellFailure(outMsg, RpcError.NO_ACTIVE_CONNECTION.name());
        then(ctxMock).should().ack(msg);
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    public void givenOriginatorIsNotDevice_whenOnMsg_thenThrowsException(EntityType entityType) {
        EntityId entityId = EntityIdFactory.getByTypeAndUuid(entityType, "ac21a1bb-eabf-4463-8313-24bea1f498d9");

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(entityId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(eq(msg), throwableCaptor.capture());
        assertThat(throwableCaptor.getValue()).isInstanceOf(RuntimeException.class)
                .hasMessage(EntityType.DEVICE != entityType ? "Message originator is not a device entity!"
                        : "Method is not present in the message!");
    }

    @ParameterizedTest
    @ValueSource(strings = {"method", "params"})
    public void givenMethodOrParamsAreNotPresent_whenOnMsg_thenThrowsException(String key) {
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data("{\"" + key + "\": \"value\"}")
                .build();

        node.onMsg(ctxMock, msg);

        ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(eq(msg), throwableCaptor.capture());
        assertThat(throwableCaptor.getValue()).isInstanceOf(RuntimeException.class)
                .hasMessage(key.equals("method") ? "Params are not present in the message!" : "Method is not present in the message!");
    }
}
