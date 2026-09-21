package com.jnks.iot.rule.engine.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleEngineRpcService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotSendRestApiCallReplyNodeTest {

    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("212445ad-9852-4bfd-819d-6b01ab6ee6b6"));

    private JnksIotSendRestApiCallReplyNode node;
    private JnksIotSendRestApiCallReplyNodeConfiguration config;
    
    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private RuleEngineRpcService rpcServiceMock;

    @BeforeEach
    public void setUp() throws JnksIotNodeException {
        node = new JnksIotSendRestApiCallReplyNode();
        config = new JnksIotSendRestApiCallReplyNodeConfiguration().defaultConfiguration();
        var configuration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctxMock, configuration);
    }

    @Test
    public void givenDefaultConfig_whenInit_thenDoesNotThrowException() {
        var configuration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        assertThatNoException().isThrownBy(() -> node.init(ctxMock, configuration));
    }

    @ParameterizedTest
    @MethodSource
    public void givenValidRestApiRequest_whenOnMsg_thenTellSuccess(String requestIdAttribute, String serviceIdAttribute) throws JnksIotNodeException {
        config.setRequestIdMetaDataAttribute(requestIdAttribute);
        config.setServiceIdMetaDataAttribute(serviceIdAttribute);
        var configuration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctxMock, configuration);
        when(ctxMock.getRpcService()).thenReturn(rpcServiceMock);
        String requestUUIDStr = "80b7883b-7ec6-4872-9dd3-b2afd5660fa6";
        String serviceIdStr = "jnks-iot-core-0";
        String data = """
                {
                "temperature": 23,
                }
                """;
        Map<String, String> metadata = Map.of(
                requestIdAttribute, requestUUIDStr,
                serviceIdAttribute, serviceIdStr);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.REST_API_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(new JnksIotMsgMetaData(metadata))
                .data(data)
                .build();

        node.onMsg(ctxMock, msg);

        UUID requestUUID = UUID.fromString(requestUUIDStr);
        verify(rpcServiceMock).sendRestApiCallReply(serviceIdStr, requestUUID, msg);
        verify(ctxMock).tellSuccess(msg);
    }

    private static Stream<Arguments> givenValidRestApiRequest_whenOnMsg_thenTellSuccess() {
        return Stream.of(
                Arguments.of("requestId", "service"),
                Arguments.of("requestUUID", "serviceId"),
                Arguments.of("some_custom_request_id_field", "some_custom_service_id_field")
        );
    }

    @ParameterizedTest
    @MethodSource
    public void givenInvalidRequest_whenOnMsg_thenTellFailure(JnksIotMsgMetaData metaData, String data, String errorMsg) {
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.REST_API_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data(data)
                .build();

        node.onMsg(ctxMock, msg);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(ctxMock).tellFailure(eq(msg), captor.capture());
        Throwable throwable = captor.getValue();
        assertThat(throwable).isInstanceOf(RuntimeException.class).hasMessage(errorMsg);
    }

    private static Stream<Arguments> givenInvalidRequest_whenOnMsg_thenTellFailure() {
        return Stream.of(
                Arguments.of(JnksIotMsgMetaData.EMPTY, JnksIotMsg.EMPTY_STRING, "Request id is not present in the metadata!"),
                Arguments.of(new JnksIotMsgMetaData(Map.of("requestUUID", "e1dd3985-efad-45a0-b0d2-0ff5dff2ccac")),
                        JnksIotMsg.EMPTY_STRING, "Service id is not present in the metadata!"),
                Arguments.of(new JnksIotMsgMetaData(Map.of("serviceId", "jnks-iot-core-0")),
                        JnksIotMsg.EMPTY_STRING, "Request id is not present in the metadata!"),
                Arguments.of(new JnksIotMsgMetaData(Map.of("requestUUID", "e1dd3985-efad-45a0-b0d2-0ff5dff2ccac", "serviceId", "jnks-iot-core-0")),
                        JnksIotMsg.EMPTY_STRING, "Request body is empty!")
        );
    }
}
