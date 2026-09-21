package com.jnks.iot.rule.engine.rpc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleEngineRpcService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgDataType;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotSendRPCReplyNodeTest {

    private static final String DUMMY_SERVICE_ID = "testServiceId";
    private static final int DUMMY_REQUEST_ID = 0;
    private static final UUID DUMMY_SESSION_ID = UUID.fromString("4f1d94aa-f6ee-4078-8499-b8e68443f8ad");
    private final String DUMMY_DATA = "{\"key\":\"value\"}";

    private JnksIotSendRPCReplyNode node;
    private JnksIotSendRpcReplyNodeConfiguration config;

    private final DeviceId deviceId = new DeviceId(UUID.fromString("af64d1b9-8635-47e1-8738-6389df7fe57e"));

    @Mock
    private JnksIotContext ctx;

    @Mock
    private RuleEngineRpcService rpcService;

    @BeforeEach
    public void setUp() throws JnksIotNodeException {
        node = new JnksIotSendRPCReplyNode();
        config = new JnksIotSendRpcReplyNodeConfiguration().defaultConfiguration();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
    }

    @Test
    public void sendReplyToTransport() {
        when(ctx.getRpcService()).thenReturn(rpcService);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(deviceId)
                .copyMetaData(getDefaultMetadata())
                .dataType(JnksIotMsgDataType.JSON)
                .data(DUMMY_DATA)
                .build();

        node.onMsg(ctx, msg);

        verify(rpcService).sendRpcReplyToDevice(DUMMY_SERVICE_ID, DUMMY_SESSION_ID, DUMMY_REQUEST_ID, DUMMY_DATA);
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    public void testOriginatorEntityTypes(EntityType entityType) {
        EntityId entityId = EntityIdFactory.getByTypeAndUuid(entityType, "0f386739-210f-4e23-8739-23f84a172adc");
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(entityId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(ctx).tellFailure(eq(msg), throwableCaptor.capture());
        assertThat(throwableCaptor.getValue()).isInstanceOf(RuntimeException.class)
                .hasMessage(EntityType.DEVICE != entityType ? "Message originator is not a device entity!"
                        : "Request id is not present in the metadata!");
    }

    @ParameterizedTest
    @MethodSource
    public void testForAvailabilityOfMetadataAndDataValues(JnksIotMsgMetaData metaData, String errorMsg) {
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(deviceId)
                .copyMetaData(metaData)
                .data(JnksIotMsg.EMPTY_STRING)
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(ctx).tellFailure(eq(msg), throwableCaptor.capture());
        assertThat(throwableCaptor.getValue()).isInstanceOf(RuntimeException.class).hasMessage(errorMsg);
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(config.getServiceIdMetaDataAttribute()).isEqualTo("serviceId");
        assertThat(config.getSessionIdMetaDataAttribute()).isEqualTo("sessionId");
        assertThat(config.getRequestIdMetaDataAttribute()).isEqualTo("requestId");
    }

    private static Stream<Arguments> testForAvailabilityOfMetadataAndDataValues() {
        return Stream.of(
                Arguments.of(JnksIotMsgMetaData.EMPTY, "Request id is not present in the metadata!"),
                Arguments.of(new JnksIotMsgMetaData(Map.of(
                        "requestId", Integer.toString(DUMMY_REQUEST_ID))), "Service id is not present in the metadata!"),
                Arguments.of(new JnksIotMsgMetaData(Map.of(
                        "requestId", Integer.toString(DUMMY_REQUEST_ID),
                        "serviceId", DUMMY_SERVICE_ID)), "Session id is not present in the metadata!"),
                Arguments.of(new JnksIotMsgMetaData(Map.of(
                        "requestId", Integer.toString(DUMMY_REQUEST_ID),
                        "serviceId", DUMMY_SERVICE_ID, "sessionId",
                        DUMMY_SESSION_ID.toString())), "Request body is empty!")
        );
    }

    private JnksIotMsgMetaData getDefaultMetadata() {
        JnksIotSendRpcReplyNodeConfiguration config = new JnksIotSendRpcReplyNodeConfiguration().defaultConfiguration();
        JnksIotMsgMetaData metadata = new JnksIotMsgMetaData();
        metadata.putValue(config.getServiceIdMetaDataAttribute(), DUMMY_SERVICE_ID);
        metadata.putValue(config.getSessionIdMetaDataAttribute(), DUMMY_SESSION_ID.toString());
        metadata.putValue(config.getRequestIdMetaDataAttribute(), Integer.toString(DUMMY_REQUEST_ID));
        return metadata;
    }
}
