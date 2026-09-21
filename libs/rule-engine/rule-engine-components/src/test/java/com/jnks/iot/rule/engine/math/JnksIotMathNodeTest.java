package com.jnks.iot.rule.engine.math;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Futures;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.verification.Timeout;
import com.jnks.iot.common.util.AbstractListeningExecutor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.AttributesSaveRequest;
import com.jnks.iot.rule.engine.api.RuleEngineTelemetryService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.DoubleDataEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.LongDataEntry;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.timeseries.TimeseriesService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class JnksIotMathNodeTest {

    static final int RULE_DISPATCHER_POOL_SIZE = 3;
    static final int DB_CALLBACK_POOL_SIZE = 4;
    static final long TIMEOUT = TimeUnit.SECONDS.toMillis(5);
    private final EntityId originator = DeviceId.fromString("ccd71696-0586-422d-940e-755a41ec3b0d");
    private final TenantId tenantId = TenantId.fromUUID(UUID.fromString("e7f46b23-0c7d-42f5-9b06-fc35ab17af8a"));

    @Mock(strictness = Mock.Strictness.LENIENT)
    private JnksIotContext ctx;
    @Mock
    private AttributesService attributesService;
    @Mock
    private TimeseriesService tsService;
    @Mock
    private RuleEngineTelemetryService telemetryService;
    private AbstractListeningExecutor dbCallbackExecutor;
    private AbstractListeningExecutor ruleEngineDispatcherExecutor;

    @BeforeEach
    public void before() {
        dbCallbackExecutor = new DBCallbackExecutor();
        dbCallbackExecutor.init();
        ruleEngineDispatcherExecutor = new RuleDispatcherExecutor();
        ruleEngineDispatcherExecutor.init();

        willReturn(dbCallbackExecutor).given(ctx).getDbCallbackExecutor();
        willReturn(attributesService).given(ctx).getAttributesService();
        willReturn(telemetryService).given(ctx).getTelemetryService();
        willReturn(tsService).given(ctx).getTimeseriesService();
        willReturn(tenantId).given(ctx).getTenantId();
    }

    @AfterEach
    public void after() {
        // shutdownNow makes some tests flaky
        ruleEngineDispatcherExecutor.destroy();
        dbCallbackExecutor.destroy();
    }

    private JnksIotMathNode initNode(JnksIotRuleNodeMathFunctionType operation, JnksIotMathResult result, JnksIotMathArgument... arguments) {
        return initNode(operation, null, result, arguments);
    }

    private JnksIotMathNode initNodeWithCustomFunction(String expression, JnksIotMathResult result, JnksIotMathArgument... arguments) {
        return initNode(JnksIotRuleNodeMathFunctionType.CUSTOM, expression, result, arguments);
    }

    private JnksIotMathNode initNodeWithCustomFunction(JnksIotContext ctx, String expression, JnksIotMathResult result, JnksIotMathArgument... arguments) {
        return initNode(ctx, JnksIotRuleNodeMathFunctionType.CUSTOM, expression, result, arguments);
    }

    private JnksIotMathNode initNode(JnksIotRuleNodeMathFunctionType operation, String expression, JnksIotMathResult result, JnksIotMathArgument... arguments) {
        return initNode(this.ctx, operation, expression, result, arguments);
    }

    private JnksIotMathNode initNode(JnksIotContext ctx, JnksIotRuleNodeMathFunctionType operation, String expression, JnksIotMathResult result, JnksIotMathArgument... arguments) {
        try {
            JnksIotMathNodeConfiguration configuration = new JnksIotMathNodeConfiguration();
            configuration.setOperation(operation);
            if (JnksIotRuleNodeMathFunctionType.CUSTOM.equals(operation)) {
                configuration.setCustomFunction(expression);
            }
            configuration.setResult(result);
            configuration.setArguments(Arrays.asList(arguments));
            JnksIotMathNode node = new JnksIotMathNode();
            node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(configuration)));
            return node;
        } catch (JnksIotNodeException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Test
    public void testExp4j() {
        var node = initNodeWithCustomFunction("2a+3b",
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "${key1}", 2, false, false, null),
                new JnksIotMathArgument("a", JnksIotMathArgumentType.MESSAGE_BODY, "${key2}"),
                new JnksIotMathArgument("b", JnksIotMathArgumentType.MESSAGE_BODY, "$[key3]")
        );

        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("key1", "firstMsgResult");
        metaData.putValue("key2", "argumentA");
        ObjectNode msgNode = JacksonUtil.newObjectNode()
                .put("key3", "argumentB").put("argumentA", 2).put("argumentB", 2);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(metaData)
                .data(msgNode.toString())
                .build();

        node.onMsg(ctx, msg);

        metaData.putValue("key1", "secondMsgResult");
        metaData.putValue("key2", "argumentC");
        msgNode = JacksonUtil.newObjectNode()
                .put("key3", "argumentD").put("argumentC", 4).put("argumentD", 3);
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(metaData)
                .data(msgNode.toString())
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT).times(2)).tellSuccess(msgCaptor.capture());

        List<JnksIotMsg> resultMsgs = msgCaptor.getAllValues();
        assertFalse(resultMsgs.isEmpty());
        assertEquals(2, resultMsgs.size());

        for (int i = 0; i < resultMsgs.size(); i++) {
            JnksIotMsg outMsg = resultMsgs.get(i);
            assertNotNull(outMsg);
            assertNotNull(outMsg.getData());
            var resultJson = JacksonUtil.toJsonNode(outMsg.getData());
            String resultKey = i == 0 ? "firstMsgResult" : "secondMsgResult";
            assertTrue(resultJson.has(resultKey));
            assertEquals(i == 0 ? 10 : 17, resultJson.get(resultKey).asInt());
        }
    }

    private static Stream<Arguments> testSimpleTwoArgumentFunction() {
        return Stream.of(
                Arguments.of(JnksIotRuleNodeMathFunctionType.ADD, 2.1, 2.2, 4.3),
                Arguments.of(JnksIotRuleNodeMathFunctionType.SUB, 2.1, 2.2, -0.1),
                Arguments.of(JnksIotRuleNodeMathFunctionType.MULT, 2.1, 2.0, 4.2),
                Arguments.of(JnksIotRuleNodeMathFunctionType.DIV, 4.2, 2.0, 2.1),
                Arguments.of(JnksIotRuleNodeMathFunctionType.ATAN2, 0.5, 0.3, 1.03),
                Arguments.of(JnksIotRuleNodeMathFunctionType.HYPOT, 4, 5, 6.4),
                Arguments.of(JnksIotRuleNodeMathFunctionType.FLOOR_DIV, 5, 3, 1),
                Arguments.of(JnksIotRuleNodeMathFunctionType.FLOOR_MOD, 6, 3, 0),
                Arguments.of(JnksIotRuleNodeMathFunctionType.MIN, 5, 3, 3),
                Arguments.of(JnksIotRuleNodeMathFunctionType.MAX, 5, 3, 5),
                Arguments.of(JnksIotRuleNodeMathFunctionType.POW, 5, 3, 125)
        );
    }

    @ParameterizedTest
    @MethodSource
    public void testSimpleTwoArgumentFunction(JnksIotRuleNodeMathFunctionType function, double arg1, double arg2, double result) {
        var node = initNode(function,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 2, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a"),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "b")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", arg1).put("b", arg2).toString())
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT).times(1)).tellSuccess(msgCaptor.capture());

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var resultJson = JacksonUtil.toJsonNode(resultMsg.getData());
        assertTrue(resultJson.has("result"));
        assertEquals(result, resultJson.get("result").asDouble(), 0d);
    }

    private static Stream<Arguments> testSimpleOneArgumentFunction() {
        return Stream.of(
                Arguments.of(JnksIotRuleNodeMathFunctionType.SIN, Math.toRadians(30), 0.5),
                Arguments.of(JnksIotRuleNodeMathFunctionType.SIN, Math.toRadians(90), 1.0),

                Arguments.of(JnksIotRuleNodeMathFunctionType.SINH, Math.toRadians(0), 0.0),
                Arguments.of(JnksIotRuleNodeMathFunctionType.COSH, Math.toRadians(0), 1.0),

                Arguments.of(JnksIotRuleNodeMathFunctionType.COS, Math.toRadians(60), 0.5),
                Arguments.of(JnksIotRuleNodeMathFunctionType.COS, Math.toRadians(0), 1.0),

                Arguments.of(JnksIotRuleNodeMathFunctionType.TAN, Math.toRadians(45), 1),
                Arguments.of(JnksIotRuleNodeMathFunctionType.TAN, Math.toRadians(0), 0),
                Arguments.of(JnksIotRuleNodeMathFunctionType.TANH, 90, 1),

                Arguments.of(JnksIotRuleNodeMathFunctionType.ACOS, 0.5, 1.05),
                Arguments.of(JnksIotRuleNodeMathFunctionType.ASIN, 0.5, 0.52),
                Arguments.of(JnksIotRuleNodeMathFunctionType.ATAN, 0.5, 0.46),

                Arguments.of(JnksIotRuleNodeMathFunctionType.EXP, 1, 2.72),
                Arguments.of(JnksIotRuleNodeMathFunctionType.EXPM1, 1, 1.72),
                Arguments.of(JnksIotRuleNodeMathFunctionType.ABS, -1, 1),
                Arguments.of(JnksIotRuleNodeMathFunctionType.SQRT, 4, 2),
                Arguments.of(JnksIotRuleNodeMathFunctionType.CBRT, 8, 2),

                Arguments.of(JnksIotRuleNodeMathFunctionType.GET_EXP, 4, 2),

                Arguments.of(JnksIotRuleNodeMathFunctionType.LOG, 4, 1.39),
                Arguments.of(JnksIotRuleNodeMathFunctionType.LOG10, 4, 0.6),
                Arguments.of(JnksIotRuleNodeMathFunctionType.LOG1P, 4, 1.61),

                Arguments.of(JnksIotRuleNodeMathFunctionType.CEIL, 1.55, 2),
                Arguments.of(JnksIotRuleNodeMathFunctionType.FLOOR, 23.97, 23),

                Arguments.of(JnksIotRuleNodeMathFunctionType.SIGNUM, 0.55, 1),
                Arguments.of(JnksIotRuleNodeMathFunctionType.RAD, 5, 0.09),
                Arguments.of(JnksIotRuleNodeMathFunctionType.DEG, 5, 286.48)
        );
    }

    @ParameterizedTest
    @MethodSource
    public void testSimpleOneArgumentFunction(JnksIotRuleNodeMathFunctionType function, double arg1, double result) {
        var node = initNode(function,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 2, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", arg1).toString())
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT).times(1)).tellSuccess(msgCaptor.capture());

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var resultJson = JacksonUtil.toJsonNode(resultMsg.getData());
        assertTrue(resultJson.has("result"));
        assertEquals(result, resultJson.get("result").asDouble(), 0d);
    }

    @Test
    public void test_2_plus_2_body() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.ADD,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 2, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a"),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "b")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 2).put("b", 2).toString())
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var resultJson = JacksonUtil.toJsonNode(resultMsg.getData());
        assertTrue(resultJson.has("result"));
        assertEquals(4, resultJson.get("result").asInt());
    }

    @Test
    public void test_2_plus_2_meta() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.ADD,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_METADATA, "result", 0, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a"),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "b")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 2).put("b", 2).toString())
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        assertNotNull(resultMsg.getMetaData());
        var result = resultMsg.getMetaData().getValue("result");
        assertNotNull(result);
        assertEquals("4", result);
    }

    @Test
    public void test_2_plus_2_attr_and_ts() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.ADD,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 2, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.ATTRIBUTE, "a"),
                new JnksIotMathArgument(JnksIotMathArgumentType.TIME_SERIES, "b")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().toString())
                .build();

        when(attributesService.find(tenantId, originator, AttributeScope.SERVER_SCOPE, "a"))
                .thenReturn(Futures.immediateFuture(Optional.of(new BaseAttributeKvEntry(System.currentTimeMillis(), new DoubleDataEntry("a", 2.0)))));

        when(tsService.findLatest(tenantId, originator, "b"))
                .thenReturn(Futures.immediateFuture(Optional.of(new BasicTsKvEntry(System.currentTimeMillis(), new LongDataEntry("b", 2L)))));

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var resultJson = JacksonUtil.toJsonNode(resultMsg.getData());
        assertTrue(resultJson.has("result"));
        assertEquals(4, resultJson.get("result").asInt());
    }

    @Test
    public void test_sqrt_5_body() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.SQRT,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 3, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 5).toString())
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var resultJson = JacksonUtil.toJsonNode(resultMsg.getData());
        assertTrue(resultJson.has("result"));
        assertEquals(2.236, resultJson.get("result").asDouble(), 0.0);
    }

    @Test
    public void test_sqrt_5_meta() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.SQRT,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_METADATA, "result", 3, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 5).toString())
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var result = resultMsg.getMetaData().getValue("result");
        assertNotNull(result);
        assertEquals("2.236", result);
    }

    @Test
    public void test_sqrt_5_to_attribute_and_metadata() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.SQRT,
                new JnksIotMathResult(JnksIotMathArgumentType.ATTRIBUTE, "result", 3, false, true, DataConstants.SERVER_SCOPE),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 5).toString())
                .build();
        doAnswer(invocation -> {
            AttributesSaveRequest request = invocation.getArgument(0);
            request.getCallback().onSuccess(null);
            return null;
        }).when(telemetryService).saveAttributes(any(AttributesSaveRequest.class));

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());
        verify(telemetryService, times(1)).saveAttributes(assertArg(request -> {
            assertThat(request.getEntries()).singleElement().extracting(KvEntry::getValue).isInstanceOf(Double.class);
        }));

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var result = resultMsg.getMetaData().getValue("result");
        assertNotNull(result);
        assertEquals("2.236", result);
    }

    @Test
    public void test_sqrt_5_to_timeseries_and_data() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.SQRT,
                new JnksIotMathResult(JnksIotMathArgumentType.TIME_SERIES, "result", 3, true, false, DataConstants.SERVER_SCOPE),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 5).toString())
                .build();
        doAnswer(invocation -> {
            TimeseriesSaveRequest request = invocation.getArgument(0);
            request.getCallback().onSuccess(null);
            return null;
        }).when(telemetryService).saveTimeseries(any(TimeseriesSaveRequest.class));

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());
        verify(telemetryService, times(1)).saveTimeseries(assertArg(request -> {
            assertThat(request.getEntries()).size().isOne();
            assertThat(request.getStrategy()).isEqualTo(TimeseriesSaveRequest.Strategy.PROCESS_ALL);
        }));

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var resultJson = JacksonUtil.toJsonNode(resultMsg.getData());
        assertTrue(resultJson.has("result"));
        assertEquals(2.236, resultJson.get("result").asDouble(), 0.0);
    }

    @Test
    public void test_sqrt_5_to_timeseries_and_metadata_and_data() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.SQRT,
                new JnksIotMathResult(JnksIotMathArgumentType.TIME_SERIES, "result", 3, true, true, DataConstants.SERVER_SCOPE),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 5).toString())
                .build();
        doAnswer(invocation -> {
            TimeseriesSaveRequest request = invocation.getArgument(0);
            request.getCallback().onSuccess(null);
            return null;
        }).when(telemetryService).saveTimeseries(any(TimeseriesSaveRequest.class));

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());
        verify(telemetryService, times(1)).saveTimeseries(assertArg(request -> {
            assertThat(request.getEntries()).size().isOne();
            assertThat(request.getStrategy()).isEqualTo(TimeseriesSaveRequest.Strategy.PROCESS_ALL);
        }));

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var resultMetadata = resultMsg.getMetaData().getValue("result");
        var resultData = JacksonUtil.toJsonNode(resultMsg.getData());

        assertTrue(resultData.has("result"));
        assertEquals(2.236, resultData.get("result").asDouble(), 0.0);

        assertNotNull(resultMetadata);
        assertEquals("2.236", resultMetadata);
    }

    @Test
    public void test_sqrt_5_default_value() {
        JnksIotMathArgument jnksIotMathArgument = new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey");
        jnksIotMathArgument.setDefaultValue(5.0);
        var node = initNode(JnksIotRuleNodeMathFunctionType.SQRT,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_METADATA, "result", 3, false, false, null),
                jnksIotMathArgument
        );
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 10).toString())
                .build();

        node.onMsg(ctx, msg);
        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());

        JnksIotMsg resultMsg = msgCaptor.getValue();
        assertNotNull(resultMsg);
        assertNotNull(resultMsg.getData());
        var result = resultMsg.getMetaData().getValue("result");
        assertNotNull(result);
        assertEquals("2.236", result);
    }

    @Test
    public void test_sqrt_5_default_value_failure() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.SQRT,
                new JnksIotMathResult(JnksIotMathArgumentType.TIME_SERIES, "result", 3, true, false, DataConstants.SERVER_SCOPE),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "TestKey")
        );
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.newObjectNode().put("a", 10).toString())
                .build();
        node.onMsg(ctx, msg);

        ArgumentCaptor<Throwable> tCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(ctx, timeout(TIMEOUT)).tellFailure(eq(msg), tCaptor.capture());
        assertNotNull(tCaptor.getValue().getMessage());
    }

    @Test
    public void testConvertMsgBodyIfRequiredFailure() {
        var node = initNode(JnksIotRuleNodeMathFunctionType.SQRT,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 3, true, false, DataConstants.SERVER_SCOPE),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_ARRAY)
                .build();
        node.onMsg(ctx, msg);

        ArgumentCaptor<Throwable> tCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(ctx, timeout(TIMEOUT)).tellFailure(eq(msg), tCaptor.capture());
        assertNotNull(tCaptor.getValue().getMessage());
    }

    @Test
    public void testExp4j_concurrent() {
        JnksIotMathNode node = spy(initNodeWithCustomFunction("2a+3b",
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 2, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a"),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "b")
        ));
        EntityId originatorSlow = DeviceId.fromString("7f01170d-6bba-419c-b95c-2b4c3ba32f30");
        EntityId originatorFast = DeviceId.fromString("c45360ff-7906-4102-a2ae-3495a86168d0");
        CountDownLatch slowProcessingLatch = new CountDownLatch(1);

        List<JnksIotMsg> slowMsgList = IntStream.range(0, 5)
                .mapToObj(x -> JnksIotMsg.newMsg()
                        .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                        .originator(originatorSlow)
                        .copyMetaData(JnksIotMsgMetaData.EMPTY)
                        .data(JacksonUtil.newObjectNode().put("a", 2).put("b", 2).toString())
                        .build())
                .toList();
        List<JnksIotMsg> fastMsgList = IntStream.range(0, 2)
                .mapToObj(x -> JnksIotMsg.newMsg()
                        .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                        .originator(originatorFast)
                        .copyMetaData(JnksIotMsgMetaData.EMPTY)
                        .data(JacksonUtil.newObjectNode().put("a", 2).put("b", 2).toString())
                        .build())
                .toList();

        assertThat(slowMsgList.size()).as("slow msgs >= rule-dispatcher pool size").isGreaterThanOrEqualTo(RULE_DISPATCHER_POOL_SIZE);

        log.debug("rule-dispatcher [{}], db-callback [{}], slowMsg [{}], fastMsg [{}]", RULE_DISPATCHER_POOL_SIZE, DB_CALLBACK_POOL_SIZE, slowMsgList.size(), fastMsgList.size());

        willAnswer(invocation -> {
            JnksIotMsg msg = invocation.getArgument(1);
            log.debug("\uD83D\uDC0C processMsgAsync slow originator [{}][{}]", msg.getOriginator(), msg);
            try {
                assertThat(slowProcessingLatch.await(30, TimeUnit.SECONDS)).as("await on slowProcessingLatch").isTrue();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return invocation.callRealMethod();
        }).given(node).processMsgAsync(eq(ctx), argThat(slowMsgList::contains));

        willAnswer(invocation -> {
            JnksIotMsg msg = invocation.getArgument(1);
            log.debug("\u26A1\uFE0F processMsgAsync FAST originator [{}][{}]", msg.getOriginator(), msg);
            return invocation.callRealMethod();
        }).given(node).processMsgAsync(eq(ctx), argThat(fastMsgList::contains));

        willAnswer(invocation -> {
            JnksIotMsg msg = invocation.getArgument(1);
            log.debug("submit slow originator onMsg [{}][{}]", msg.getOriginator(), msg);
            return invocation.callRealMethod();
        }).given(node).onMsg(eq(ctx), argThat(slowMsgList::contains));

        willAnswer(invocation -> {
            JnksIotMsg msg = invocation.getArgument(1);
            log.debug("submit FAST originator onMsg [{}][{}]", msg.getOriginator(), msg);
            return invocation.callRealMethod();
        }).given(node).onMsg(eq(ctx), argThat(fastMsgList::contains));

        // submit slow msg may block all rule engine dispatcher threads
        slowMsgList.forEach(msg -> ruleEngineDispatcherExecutor.executeAsync(() -> node.onMsg(ctx, msg)));
        // wait until dispatcher threads started with all slowMsg
        verify(node, timeout(TIMEOUT).times(slowMsgList.size())).onMsg(eq(ctx), argThat(slowMsgList::contains));

        // submit fast have to return immediately
        fastMsgList.forEach(msg -> ruleEngineDispatcherExecutor.executeAsync(() -> node.onMsg(ctx, msg)));
        // wait until all fast messages processed
        verify(ctx, timeout(TIMEOUT).times(fastMsgList.size())).tellSuccess(any());

        slowProcessingLatch.countDown();

        verify(ctx, timeout(TIMEOUT).times(fastMsgList.size() + slowMsgList.size())).tellSuccess(any());

        verify(ctx, never()).tellFailure(any(), any());
    }

    @Test
    public void testExp4j_concurrentBySingleOriginator_processMsgAsyncException() {
        JnksIotMathNode node = spy(initNodeWithCustomFunction("2a+3b",
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 2, false, false, null),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a"),
                new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "b")
        ));

        willThrow(new RuntimeException("Message body has no 'delta'")).given(node).resolveArguments(any(), any(), any(), any());

        EntityId originatorSlow = DeviceId.fromString("7f01170d-6bba-419c-b95c-2b4c3ba32f30");
        CountDownLatch slowProcessingLatch = new CountDownLatch(1);

        List<JnksIotMsg> slowMsgList = IntStream.range(0, 5)
                .mapToObj(x -> JnksIotMsg.newMsg()
                        .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                        .originator(originatorSlow)
                        .copyMetaData(JnksIotMsgMetaData.EMPTY)
                        .data(JacksonUtil.newObjectNode().put("a", 2).put("b", 2).toString())
                        .build())
                .collect(Collectors.toList());

        assertThat(slowMsgList.size()).as("slow msgs >= rule-dispatcher pool size").isGreaterThanOrEqualTo(RULE_DISPATCHER_POOL_SIZE);

        log.debug("rule-dispatcher [{}], db-callback [{}], slowMsg [{}]", RULE_DISPATCHER_POOL_SIZE, DB_CALLBACK_POOL_SIZE, slowMsgList.size());

        willAnswer(invocation -> {
            JnksIotMsg msg = invocation.getArgument(1);
            if (slowProcessingLatch.getCount() > 0) {
                log.debug("Await on slowProcessingLatch before processMsgAsync");
                try {
                    assertThat(slowProcessingLatch.await(30, TimeUnit.SECONDS)).as("await on slowProcessingLatch").isTrue();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            log.debug("\uD83D\uDC0C processMsgAsync with exception [{}][{}]", msg.getOriginator(), msg);
            return invocation.callRealMethod();
        }).given(node).processMsgAsync(eq(ctx), argThat(slowMsgList::contains));

        willAnswer(invocation -> {
            JnksIotMsg msg = invocation.getArgument(1);
            log.debug("submit slow originator onMsg [{}][{}]", msg.getOriginator(), msg);
            return invocation.callRealMethod();
        }).given(node).onMsg(eq(ctx), argThat(slowMsgList::contains));

        // submit slow msg may block all rule engine dispatcher threads
        slowMsgList.forEach(msg -> ruleEngineDispatcherExecutor.executeAsync(() -> node.onMsg(ctx, msg)));
        // wait until dispatcher threads started with all slowMsg
        verify(node, new Timeout(TIMEOUT, times(slowMsgList.size()))).onMsg(eq(ctx), argThat(slowMsgList::contains));

        slowProcessingLatch.countDown();

        verify(ctx, new Timeout(TIMEOUT, times(slowMsgList.size()))).tellFailure(any(), any());
        verify(ctx, never()).tellSuccess(any());

    }

    @Test
    public void testExp4j_concurrentBySingleOriginator_SingleMsg_manyNodesWithDifferentOutput() {
        assertThat(RULE_DISPATCHER_POOL_SIZE).as("dispatcher pool size have to be > 1").isGreaterThan(1);
        CountDownLatch processingLatch = new CountDownLatch(1);
        List<Triple<JnksIotContext, String, JnksIotMathNode>> ctxNodes = IntStream.range(0, RULE_DISPATCHER_POOL_SIZE * 2)
                .mapToObj(x -> {
                    final JnksIotContext ctx = mock(JnksIotContext.class); // many rule nodes - many contexts
                    willReturn(dbCallbackExecutor).given(ctx).getDbCallbackExecutor();
                    final String resultKey = "result" + x;
                    final JnksIotMathNode node = spy(initNodeWithCustomFunction(ctx, "2a+3b",
                            new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_METADATA, resultKey, 1, false, true, null),
                            new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "a"),
                            new JnksIotMathArgument(JnksIotMathArgumentType.MESSAGE_BODY, "b")));
                    willAnswer(invocation -> {
                        if (processingLatch.getCount() > 0) {
                            log.debug("Await on processingLatch before processMsgAsync");
                            try {
                                assertThat(processingLatch.await(30, TimeUnit.SECONDS)).as("await on processingLatch").isTrue();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        log.debug("\uD83D\uDC0C processMsgAsync on node with expected resultKey [{}]", resultKey);
                        return invocation.callRealMethod();
                    }).given(node).processMsgAsync(any(), any());
                    willAnswer(invocation -> {
                        JnksIotMsg msg = invocation.getArgument(1);
                        log.debug("submit originator onMsg [{}][{}]", msg.getOriginator(), msg);
                        return invocation.callRealMethod();
                    }).given(node).onMsg(any(), any());
                    return Triple.of(ctx, resultKey, node);
                })
                .toList();
        ctxNodes.forEach(ctxNode -> ruleEngineDispatcherExecutor.executeAsync(() -> ctxNode.getRight()
                .onMsg(ctxNode.getLeft(), JnksIotMsg.newMsg()
                        .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                        .originator(originator)
                        .copyMetaData(JnksIotMsgMetaData.EMPTY)
                        .data("{\"a\":2,\"b\":2}")
                        .build())));
        ctxNodes.forEach(ctxNode -> verify(ctxNode.getRight(), timeout(TIMEOUT)).onMsg(eq(ctxNode.getLeft()), any()));
        processingLatch.countDown();

        SoftAssertions softly = new SoftAssertions();
        ctxNodes.forEach(ctxNode -> {
            final JnksIotContext ctx = ctxNode.getLeft();
            final String resultKey = ctxNode.getMiddle();
            ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
            verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());

            JnksIotMsg resultMsg = msgCaptor.getValue();
            assertThat(resultMsg).as("result msg non null for result key " + resultKey).isNotNull();
            log.debug("asserting result key [{}] in metadata [{}]", resultKey, resultMsg.getMetaData().getData());
            softly.assertThat(resultMsg.getMetaData().getValue(resultKey)).as("asserting result key " + resultKey)
                    .isEqualTo("10.0");
        });

        softly.assertAll();
        verify(ctx, never()).tellFailure(any(), any());
    }

    @ParameterizedTest
    @MethodSource
    public void testCustomFunctions(String customFunction, double result) {
        var node = initNodeWithCustomFunction(customFunction,
                new JnksIotMathResult(JnksIotMathArgumentType.MESSAGE_BODY, "result", 2, false, false, null),
                new JnksIotMathArgument("a", JnksIotMathArgumentType.MESSAGE_BODY, "argumentA"),
                new JnksIotMathArgument("b", JnksIotMathArgumentType.MESSAGE_BODY, "argumentB")
        );

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .metaData(JnksIotMsgMetaData.EMPTY)
                .data("{\"argumentA\":2,\"argumentB\":5}")
                .build();

        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, timeout(TIMEOUT)).tellSuccess(msgCaptor.capture());
        JnksIotMsg outMsg = msgCaptor.getValue();
        assertThat(outMsg).isNotNull();
        assertThat(outMsg.getData()).isNotNull();
        var resultJson = JacksonUtil.toJsonNode(outMsg.getData());
        assertThat(resultJson.has("result")).isTrue();
        assertThat(resultJson.get("result").asDouble()).isEqualTo(new BigDecimal(result).setScale(2, RoundingMode.HALF_UP).doubleValue());
    }

    private static Stream<Arguments> testCustomFunctions() {
        return Stream.of(
                Arguments.of("ln(a)", Math.log(2)),
                Arguments.of("lg(a)", Math.log10(2)),
                Arguments.of("logab(a, b)", Math.log(5) / Math.log(2))
        );
    }

    static class RuleDispatcherExecutor extends AbstractListeningExecutor {
        @Override
        protected int getThreadPollSize() {
            return RULE_DISPATCHER_POOL_SIZE;
        }
    }

    static class DBCallbackExecutor extends AbstractListeningExecutor {
        @Override
        protected int getThreadPollSize() {
            return DB_CALLBACK_POOL_SIZE;
        }
    }

}
