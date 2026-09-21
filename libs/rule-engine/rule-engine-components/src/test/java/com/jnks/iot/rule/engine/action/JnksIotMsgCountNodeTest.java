package com.jnks.iot.rule.engine.action;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.times;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class JnksIotMsgCountNodeTest {

    private final RuleNodeId RULE_NODE_ID = new RuleNodeId(UUID.fromString("ee682a85-7f5a-4182-91bc-46e555138fe2"));
    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("1b21c7cc-0c9e-4ab1-b867-99451599e146"));
    private final TenantId TENANT_ID = TenantId.fromUUID(UUID.fromString("04dfbd38-10e5-47b7-925f-11e795db89e1"));

    private final JnksIotMsg tickMsg = JnksIotMsg.newMsg()
            .type(JnksIotMsgType.MSG_COUNT_SELF_MSG)
            .originator(RULE_NODE_ID)
            .copyMetaData(JnksIotMsgMetaData.EMPTY)
            .data(JnksIotMsg.EMPTY_STRING)
            .build();

    private ScheduledExecutorService executorService;
    private JnksIotMsgCountNode node;
    private JnksIotMsgCountNodeConfiguration config;

    @Mock
    private JnksIotContext ctxMock;

    @BeforeEach
    public void setUp() {
        node = new JnksIotMsgCountNode();
        config = new JnksIotMsgCountNodeConfiguration().defaultConfiguration();
        executorService = JnksIotExecutors.newSingleThreadScheduledExecutor("msg-count-node-test");
    }

    @AfterEach
    public void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
        node.destroy();
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(config.getInterval()).isEqualTo(1);
        assertThat(config.getTelemetryPrefix()).isEqualTo("messageCount");
    }

    @Test
    public void givenIncomingMsgs_whenOnMsg_thenSendsMsgWithMsgCount() throws JnksIotNodeException, InterruptedException {
        // GIVEN
        int msgCount = 100;
        var awaitTellSelfLatch = new CountDownLatch(1);
        var currentMsgNumber = new AtomicInteger(0);
        var msgWithCounterSent = new AtomicBoolean(false);

        willAnswer((Answer<Void>) invocationOnMock -> {
            executorService.schedule(() -> {
                JnksIotMsg tickMsg = invocationOnMock.getArgument(0);
                msgWithCounterSent.set(true);
                node.onMsg(ctxMock, tickMsg);
                awaitTellSelfLatch.countDown();
            }, config.getInterval(), TimeUnit.SECONDS);
            return null;
        }).given(ctxMock).tellSelf(any(JnksIotMsg.class), anyLong());
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.getServiceId()).willReturn("jnks-iot-rule-engine");
        given(ctxMock.getSelfId()).willReturn(RULE_NODE_ID);
        given(ctxMock.newMsg(null, JnksIotMsgType.MSG_COUNT_SELF_MSG, RULE_NODE_ID, null, JnksIotMsgMetaData.EMPTY, JnksIotMsg.EMPTY_STRING)).willReturn(tickMsg);

        // WHEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        var expectedProcessedMsgs = new ArrayList<JnksIotMsg>();
        for (int i = 0; i < msgCount; i++) {
            var msg = JnksIotMsg.newMsg()
                    .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                    .originator(DEVICE_ID)
                    .copyMetaData(JnksIotMsgMetaData.EMPTY)
                    .data(JnksIotMsg.EMPTY_STRING)
                    .build();
            if (msgWithCounterSent.get()) {
                break;
            }
            node.onMsg(ctxMock, msg);
            expectedProcessedMsgs.add(msg);
            currentMsgNumber.getAndIncrement();
        }

        awaitTellSelfLatch.await();

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should(times(currentMsgNumber.get())).ack(msgCaptor.capture());
        var actualProcessedMsgs = msgCaptor.getAllValues();
        assertThat(actualProcessedMsgs).hasSize(expectedProcessedMsgs.size());
        assertThat(actualProcessedMsgs).isNotEmpty();
        assertThat(actualProcessedMsgs).containsExactlyInAnyOrderElementsOf(expectedProcessedMsgs);

        ArgumentCaptor<JnksIotMsg> msgWithCounterCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().enqueueForTellNext(msgWithCounterCaptor.capture(), eq(JnksIotNodeConnectionType.SUCCESS));
        JnksIotMsg resultedMsg = msgWithCounterCaptor.getValue();
        String expectedData = "{\"messageCount_tb-rule-engine\":" + currentMsgNumber + "}";
        JnksIotMsg expectedMsg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(TENANT_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(expectedData)
                .build();
        assertThat(resultedMsg).usingRecursiveComparison()
                .ignoringFields("id", "ts", "ctx", "metaData")
                .isEqualTo(expectedMsg);
        Map<String, String> actualMetadata = resultedMsg.getMetaData().getData();
        assertThat(actualMetadata).hasFieldOrProperty("delta");
    }

}
