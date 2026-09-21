package com.jnks.iot.rule.engine.transform;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.deduplication.DeduplicationStrategy;
import com.jnks.iot.rule.engine.deduplication.JnksIotMsgDeduplicationNode;
import com.jnks.iot.rule.engine.deduplication.JnksIotMsgDeduplicationNodeConfiguration;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class JnksIotMsgDeduplicationNodeTest extends AbstractRuleNodeUpgradeTest {

    private JnksIotContext ctx;

    private final ScheduledExecutorService executorService = JnksIotExecutors.newSingleThreadScheduledExecutor("de-duplication-node-test");
    private final int deduplicationInterval = 1;

    private TenantId tenantId;

    private JnksIotMsgDeduplicationNode node;
    private JnksIotMsgDeduplicationNodeConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;

    private CountDownLatch awaitTellSelfLatch;

    @BeforeEach
    public void init() throws JnksIotNodeException {
        ctx = mock(JnksIotContext.class);

        tenantId = TenantId.fromUUID(UUID.randomUUID());
        RuleNodeId ruleNodeId = new RuleNodeId(UUID.randomUUID());

        when(ctx.getSelfId()).thenReturn(ruleNodeId);
        when(ctx.getTenantId()).thenReturn(tenantId);

        doAnswer((Answer<JnksIotMsg>) invocationOnMock -> {
            JnksIotMsgType type = (JnksIotMsgType) (invocationOnMock.getArguments())[1];
            EntityId originator = (EntityId) (invocationOnMock.getArguments())[2];
            JnksIotMsgMetaData metaData = (JnksIotMsgMetaData) (invocationOnMock.getArguments())[3];
            String data = (String) (invocationOnMock.getArguments())[4];
            return JnksIotMsg.newMsg()
                    .type(type)
                    .originator(originator)
                    .copyMetaData(metaData)
                    .data(data)
                    .build();
        }).when(ctx).newMsg(isNull(), eq(JnksIotMsgType.DEDUPLICATION_TIMEOUT_SELF_MSG), nullable(EntityId.class), any(JnksIotMsgMetaData.class), any(String.class));
        node = spy(new JnksIotMsgDeduplicationNode());
        config = new JnksIotMsgDeduplicationNodeConfiguration().defaultConfiguration();
    }

    private void invokeTellSelf(int maxNumberOfInvocation) {
        invokeTellSelf(maxNumberOfInvocation, false, 0);
    }

    private void invokeTellSelf(int maxNumberOfInvocation, boolean delayScheduleTimeout, int delayMultiplier) {
        AtomicLong scheduleTimeout = new AtomicLong(deduplicationInterval);
        AtomicInteger scheduleCount = new AtomicInteger(0);
        doAnswer((Answer<Void>) invocationOnMock -> {
            scheduleCount.getAndIncrement();
            if (scheduleCount.get() <= maxNumberOfInvocation) {
                JnksIotMsg msg = (JnksIotMsg) (invocationOnMock.getArguments())[0];
                executorService.schedule(() -> {
                    try {
                        node.onMsg(ctx, msg);
                        awaitTellSelfLatch.countDown();
                    } catch (ExecutionException | InterruptedException | JnksIotNodeException e) {
                        log.error("Failed to execute tellSelf method call due to: ", e);
                    }
                }, scheduleTimeout.get(), TimeUnit.SECONDS);
                if (delayScheduleTimeout) {
                    scheduleTimeout.set(scheduleTimeout.get() * delayMultiplier);
                }
            }

            return null;
        }).when(ctx).tellSelf(ArgumentMatchers.any(JnksIotMsg.class), ArgumentMatchers.anyLong());
    }

    @AfterEach
    public void destroy() {
        executorService.shutdown();
        node.destroy();
    }

    private static Stream<Arguments> given_100_messages_strategy_first_then_verifyOutput() {
        return Stream.of(
                Arguments.of((String) null),
                Arguments.of(DataConstants.MAIN_QUEUE_NAME),
                Arguments.of(DataConstants.HP_QUEUE_NAME)
        );
    }

    @ParameterizedTest
    @MethodSource
    public void given_100_messages_strategy_first_then_verifyOutput(String queueName) throws JnksIotNodeException, ExecutionException, InterruptedException {
        int wantedNumberOfTellSelfInvocation = 2;
        int msgCount = 100;
        awaitTellSelfLatch = new CountDownLatch(wantedNumberOfTellSelfInvocation);
        invokeTellSelf(wantedNumberOfTellSelfInvocation);

        when(ctx.getQueueName()).thenReturn(queueName);

        config.setInterval(deduplicationInterval);
        config.setMaxPendingMsgs(msgCount);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        long currentTimeMillis = System.currentTimeMillis();

        List<JnksIotMsg> inputMsgs = getJnksIotMsgs(deviceId, msgCount, currentTimeMillis, 500);
        for (JnksIotMsg msg : inputMsgs) {
            node.onMsg(ctx, msg);
        }

        JnksIotMsg msgToReject = createMsg(deviceId, inputMsgs.get(inputMsgs.size() - 1).getMetaDataTs() + 2);
        node.onMsg(ctx, msgToReject);

        awaitTellSelfLatch.await();

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Runnable> successCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Consumer<Throwable>> failureCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(ctx, times(msgCount)).ack(any());
        verify(ctx, times(1)).tellFailure(eq(msgToReject), any());
        verify(node, times(msgCount + wantedNumberOfTellSelfInvocation + 1)).onMsg(eq(ctx), any());
        verify(ctx, times(1)).enqueueForTellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.SUCCESS), successCaptor.capture(), failureCaptor.capture());

        JnksIotMsg firstMsg = inputMsgs.get(0);
        JnksIotMsg actualMsg = newMsgCaptor.getValue();
        // msg ids should be different because we create new msg before enqueueForTellNext
        Assertions.assertNotEquals(firstMsg.getId(), actualMsg.getId());
        Assertions.assertEquals(firstMsg.getOriginator(), actualMsg.getOriginator());
        Assertions.assertEquals(firstMsg.getCustomerId(), actualMsg.getCustomerId());
        Assertions.assertEquals(firstMsg.getData(), actualMsg.getData());
        Assertions.assertEquals(firstMsg.getMetaData(), actualMsg.getMetaData());
        Assertions.assertEquals(firstMsg.getType(), actualMsg.getType());

        if (queueName == null) {
            Assertions.assertEquals(firstMsg.getQueueName(), actualMsg.getQueueName());
        } else {
            Assertions.assertEquals(ctx.getQueueName(), actualMsg.getQueueName());
        }
    }

    @Test
    public void given_100_messages_strategy_last_then_verifyOutput() throws JnksIotNodeException, ExecutionException, InterruptedException {
        int wantedNumberOfTellSelfInvocation = 2;
        int msgCount = 100;
        awaitTellSelfLatch = new CountDownLatch(wantedNumberOfTellSelfInvocation);
        invokeTellSelf(wantedNumberOfTellSelfInvocation);

        config.setStrategy(DeduplicationStrategy.LAST);
        config.setInterval(deduplicationInterval);
        config.setMaxPendingMsgs(msgCount);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        long currentTimeMillis = System.currentTimeMillis();

        List<JnksIotMsg> inputMsgs = getJnksIotMsgs(deviceId, msgCount, currentTimeMillis, 500);
        JnksIotMsg msgWithLatestTs = getMsgWithLatestTs(inputMsgs);

        for (JnksIotMsg msg : inputMsgs) {
            node.onMsg(ctx, msg);
        }

        JnksIotMsg msgToReject = createMsg(deviceId, inputMsgs.get(inputMsgs.size() - 1).getMetaDataTs() + 2);
        node.onMsg(ctx, msgToReject);

        awaitTellSelfLatch.await();

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Runnable> successCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Consumer<Throwable>> failureCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(ctx, times(msgCount)).ack(any());
        verify(ctx, times(1)).tellFailure(eq(msgToReject), any());
        verify(node, times(msgCount + wantedNumberOfTellSelfInvocation + 1)).onMsg(eq(ctx), any());
        verify(ctx, times(1)).enqueueForTellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.SUCCESS), successCaptor.capture(), failureCaptor.capture());

        JnksIotMsg actualMsg = newMsgCaptor.getValue();
        // msg ids should be different because we create new msg before enqueueForTellNext
        Assertions.assertNotEquals(msgWithLatestTs.getId(), actualMsg.getId());
        Assertions.assertEquals(msgWithLatestTs.getOriginator(), actualMsg.getOriginator());
        Assertions.assertEquals(msgWithLatestTs.getCustomerId(), actualMsg.getCustomerId());
        Assertions.assertEquals(msgWithLatestTs.getData(), actualMsg.getData());
        Assertions.assertEquals(msgWithLatestTs.getMetaData(), actualMsg.getMetaData());
        Assertions.assertEquals(msgWithLatestTs.getType(), actualMsg.getType());
    }

    @Test
    public void given_100_messages_strategy_all_then_verifyOutput() throws JnksIotNodeException, ExecutionException, InterruptedException {
        int wantedNumberOfTellSelfInvocation = 2;
        int msgCount = 100;
        awaitTellSelfLatch = new CountDownLatch(wantedNumberOfTellSelfInvocation);
        invokeTellSelf(wantedNumberOfTellSelfInvocation);

        when(ctx.getQueueName()).thenReturn(DataConstants.HP_QUEUE_NAME);
        config.setInterval(deduplicationInterval);
        config.setStrategy(DeduplicationStrategy.ALL);
        config.setOutMsgType(JnksIotMsgType.POST_ATTRIBUTES_REQUEST.name());
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        long currentTimeMillis = System.currentTimeMillis();

        List<JnksIotMsg> inputMsgs = getJnksIotMsgs(deviceId, msgCount, currentTimeMillis, 500);
        for (JnksIotMsg msg : inputMsgs) {
            node.onMsg(ctx, msg);
        }

        awaitTellSelfLatch.await();

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Runnable> successCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Consumer<Throwable>> failureCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(ctx, times(msgCount)).ack(any());
        verify(node, times(msgCount + wantedNumberOfTellSelfInvocation)).onMsg(eq(ctx), any());
        verify(ctx, times(1)).enqueueForTellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.SUCCESS), successCaptor.capture(), failureCaptor.capture());

        Assertions.assertEquals(1, newMsgCaptor.getAllValues().size());
        JnksIotMsg outMessage = newMsgCaptor.getAllValues().get(0);
        Assertions.assertEquals(getMergedData(inputMsgs), outMessage.getData());
        Assertions.assertEquals(deviceId, outMessage.getOriginator());
        Assertions.assertEquals(config.getOutMsgType(), outMessage.getType());
        Assertions.assertEquals(DataConstants.HP_QUEUE_NAME, outMessage.getQueueName());
    }

    @Test
    public void given_100_messages_strategy_all_then_verifyOutput_2_packs() throws JnksIotNodeException, ExecutionException, InterruptedException {
        int wantedNumberOfTellSelfInvocation = 2;
        int msgCount = 100;
        awaitTellSelfLatch = new CountDownLatch(wantedNumberOfTellSelfInvocation);
        invokeTellSelf(wantedNumberOfTellSelfInvocation, true, 3);

        when(ctx.getQueueName()).thenReturn(DataConstants.HP_QUEUE_NAME);
        config.setInterval(deduplicationInterval);
        config.setStrategy(DeduplicationStrategy.ALL);
        config.setOutMsgType(JnksIotMsgType.POST_ATTRIBUTES_REQUEST.name());
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        long currentTimeMillis = System.currentTimeMillis();

        List<JnksIotMsg> firstMsgPack = getJnksIotMsgs(deviceId, msgCount / 2, currentTimeMillis, 500);
        for (JnksIotMsg msg : firstMsgPack) {
            node.onMsg(ctx, msg);
        }
        long firstPackDeduplicationPackEndTs = firstMsgPack.get(0).getMetaDataTs() + TimeUnit.SECONDS.toMillis(deduplicationInterval);

        List<JnksIotMsg> secondMsgPack = getJnksIotMsgs(deviceId, msgCount / 2, firstPackDeduplicationPackEndTs, 500);
        for (JnksIotMsg msg : secondMsgPack) {
            node.onMsg(ctx, msg);
        }

        awaitTellSelfLatch.await();

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Runnable> successCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Consumer<Throwable>> failureCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(ctx, times(msgCount)).ack(any());
        verify(node, times(msgCount + wantedNumberOfTellSelfInvocation)).onMsg(eq(ctx), any());
        verify(ctx, times(2)).enqueueForTellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.SUCCESS), successCaptor.capture(), failureCaptor.capture());

        List<JnksIotMsg> resultMsgs = newMsgCaptor.getAllValues();
        Assertions.assertEquals(2, resultMsgs.size());

        JnksIotMsg firstMsg = resultMsgs.get(0);
        Assertions.assertEquals(getMergedData(firstMsgPack), firstMsg.getData());
        Assertions.assertEquals(deviceId, firstMsg.getOriginator());
        Assertions.assertEquals(config.getOutMsgType(), firstMsg.getType());
        Assertions.assertEquals(DataConstants.HP_QUEUE_NAME, firstMsg.getQueueName());

        JnksIotMsg secondMsg = resultMsgs.get(1);
        Assertions.assertEquals(getMergedData(secondMsgPack), secondMsg.getData());
        Assertions.assertEquals(deviceId, secondMsg.getOriginator());
        Assertions.assertEquals(config.getOutMsgType(), secondMsg.getType());
        Assertions.assertEquals(DataConstants.HP_QUEUE_NAME, secondMsg.getQueueName());
    }

    @Test
    public void given_100_messages_strategy_last_then_verifyOutput_2_packs() throws JnksIotNodeException, ExecutionException, InterruptedException {
        int wantedNumberOfTellSelfInvocation = 2;
        int msgCount = 100;
        awaitTellSelfLatch = new CountDownLatch(wantedNumberOfTellSelfInvocation);
        invokeTellSelf(wantedNumberOfTellSelfInvocation, true, 3);

        config.setInterval(deduplicationInterval);
        config.setStrategy(DeduplicationStrategy.LAST);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctx, nodeConfiguration);

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        long currentTimeMillis = System.currentTimeMillis();

        List<JnksIotMsg> firstMsgPack = getJnksIotMsgs(deviceId, msgCount / 2, currentTimeMillis, 500);
        for (JnksIotMsg msg : firstMsgPack) {
            node.onMsg(ctx, msg);
        }
        long firstPackDeduplicationPackEndTs = firstMsgPack.get(0).getMetaDataTs() + TimeUnit.SECONDS.toMillis(deduplicationInterval);
        JnksIotMsg msgWithLatestTsInFirstPack = getMsgWithLatestTs(firstMsgPack);

        List<JnksIotMsg> secondMsgPack = getJnksIotMsgs(deviceId, msgCount / 2, firstPackDeduplicationPackEndTs, 500);
        for (JnksIotMsg msg : secondMsgPack) {
            node.onMsg(ctx, msg);
        }
        JnksIotMsg msgWithLatestTsInSecondPack = getMsgWithLatestTs(secondMsgPack);

        awaitTellSelfLatch.await();

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Runnable> successCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Consumer<Throwable>> failureCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(ctx, times(msgCount)).ack(any());
        verify(node, times(msgCount + wantedNumberOfTellSelfInvocation)).onMsg(eq(ctx), any());
        verify(ctx, times(2)).enqueueForTellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.SUCCESS), successCaptor.capture(), failureCaptor.capture());

        List<JnksIotMsg> resultMsgs = newMsgCaptor.getAllValues();
        Assertions.assertEquals(2, resultMsgs.size());

        // verify that newMsg is called but content of messages is the same as in the last msg for the first pack.
        JnksIotMsg actualMsg = resultMsgs.get(0);
        Assertions.assertNotEquals(msgWithLatestTsInFirstPack.getId(), actualMsg.getId());
        Assertions.assertEquals(msgWithLatestTsInFirstPack.getOriginator(), actualMsg.getOriginator());
        Assertions.assertEquals(msgWithLatestTsInFirstPack.getCustomerId(), actualMsg.getCustomerId());
        Assertions.assertEquals(msgWithLatestTsInFirstPack.getData(), actualMsg.getData());
        Assertions.assertEquals(msgWithLatestTsInFirstPack.getMetaData(), actualMsg.getMetaData());
        Assertions.assertEquals(msgWithLatestTsInFirstPack.getType(), actualMsg.getType());

        // verify that newMsg is called but content of messages is the same as in the last msg for the second pack.
        actualMsg = resultMsgs.get(1);
        Assertions.assertNotEquals(msgWithLatestTsInSecondPack.getId(), actualMsg.getId());
        Assertions.assertEquals(msgWithLatestTsInSecondPack.getOriginator(), actualMsg.getOriginator());
        Assertions.assertEquals(msgWithLatestTsInSecondPack.getCustomerId(), actualMsg.getCustomerId());
        Assertions.assertEquals(msgWithLatestTsInSecondPack.getData(), actualMsg.getData());
        Assertions.assertEquals(msgWithLatestTsInSecondPack.getMetaData(), actualMsg.getMetaData());
        Assertions.assertEquals(msgWithLatestTsInSecondPack.getType(), actualMsg.getType());
    }

    // Rule nodes upgrade
    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                // default config for version 0
                Arguments.of(0,
                        "{\"interval\":60,\"strategy\":\"FIRST\",\"outMsgType\":null,\"maxPendingMsgs\":100,\"maxRetries\":3, \"queueName\":null}",
                        true,
                        "{\"interval\":60,\"strategy\":\"FIRST\",\"outMsgType\":null,\"maxPendingMsgs\":100,\"maxRetries\":3}"),
                // default config for version 0 with queueName
                Arguments.of(0,
                        "{\"interval\":60,\"strategy\":\"FIRST\",\"outMsgType\":null,\"maxPendingMsgs\":100,\"maxRetries\":3, \"queueName\":\"Main\"}",
                        true,
                        "{\"interval\":60,\"strategy\":\"FIRST\",\"outMsgType\":null,\"maxPendingMsgs\":100,\"maxRetries\":3}"),
                // default config for version 1 with upgrade from version 0
                Arguments.of(0,
                        "{\"interval\":60,\"strategy\":\"FIRST\",\"outMsgType\":null,\"maxPendingMsgs\":100,\"maxRetries\":3}",
                        false,
                        "{\"interval\":60,\"strategy\":\"FIRST\",\"outMsgType\":null,\"maxPendingMsgs\":100,\"maxRetries\":3}")
        );
    }

    private JnksIotMsg getMsgWithLatestTs(List<JnksIotMsg> firstMsgPack) {
        int indexOfLastMsgInArray = firstMsgPack.size() - 1;
        int indexToSetMaxTs = new Random().nextInt(indexOfLastMsgInArray) + 1;
        JnksIotMsg currentMaxTsMsg = firstMsgPack.get(indexOfLastMsgInArray);
        JnksIotMsg newLastMsgOfArray = firstMsgPack.get(indexToSetMaxTs);
        firstMsgPack.set(indexOfLastMsgInArray, newLastMsgOfArray);
        firstMsgPack.set(indexToSetMaxTs, currentMaxTsMsg);
        return currentMaxTsMsg;
    }

    private List<JnksIotMsg> getJnksIotMsgs(DeviceId deviceId, int msgCount, long currentTimeMillis, int initTsStep) {
        List<JnksIotMsg> inputMsgs = new ArrayList<>();
        var ts = currentTimeMillis + initTsStep;
        for (int i = 0; i < msgCount; i++) {
            inputMsgs.add(createMsg(deviceId, ts));
            ts += 2;
        }
        return inputMsgs;
    }

    private JnksIotMsg createMsg(DeviceId deviceId, long ts) {
        ObjectNode dataNode = JacksonUtil.newObjectNode();
        dataNode.put("deviceId", deviceId.getId().toString());
        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("ts", String.valueOf(ts));
        return JnksIotMsg.newMsg()
                .queueName(DataConstants.MAIN_QUEUE_NAME)
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(deviceId)
                .copyMetaData(metaData)
                .data(JacksonUtil.toString(dataNode))
                .build();
    }

    private String getMergedData(List<JnksIotMsg> msgs) {
        ArrayNode mergedData = JacksonUtil.newArrayNode();
        msgs.forEach(msg -> {
            ObjectNode msgNode = JacksonUtil.newObjectNode();
            msgNode.set("msg", JacksonUtil.toJsonNode(msg.getData()));
            msgNode.set("metadata", JacksonUtil.valueToTree(msg.getMetaData().getData()));
            mergedData.add(msgNode);
        });
        return JacksonUtil.toString(mergedData);
    }

    @Override
    protected JnksIotNode getTestNode() {
        return node;
    }

}
