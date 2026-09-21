package com.jnks.iot.rule.engine.filter;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.RuleEngineAlarmService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JnksIotCheckAlarmStatusNodeTest {

    private static final TenantId TENANT_ID = new TenantId(UUID.randomUUID());
    private static final DeviceId DEVICE_ID = new DeviceId(UUID.randomUUID());
    private static final AlarmId ALARM_ID = new AlarmId(UUID.randomUUID());
    private static final TestDbCallbackExecutor DB_EXECUTOR = new TestDbCallbackExecutor();

    private JnksIotCheckAlarmStatusNode node;

    private JnksIotContext ctx;
    private RuleEngineAlarmService alarmService;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        var config = new JnksIotCheckAlarmStatusNodeConfig().defaultConfiguration();

        ctx = mock(JnksIotContext.class);
        alarmService = mock(RuleEngineAlarmService.class);

        when(ctx.getTenantId()).thenReturn(TENANT_ID);
        when(ctx.getAlarmService()).thenReturn(alarmService);
        when(ctx.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        node = new JnksIotCheckAlarmStatusNode();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenActiveAlarm_whenOnMsg_then_True() throws JnksIotNodeException {
        // GIVEN
        var alarm = new Alarm();
        alarm.setId(ALARM_ID);
        alarm.setOriginator(DEVICE_ID);
        alarm.setType("General Alarm");

        String msgData = JacksonUtil.toString(alarm);
        JnksIotMsg msg = getJnksIotMsg(msgData);

        when(alarmService.findAlarmByIdAsync(TENANT_ID, ALARM_ID)).thenReturn(Futures.immediateFuture(alarm));

        // WHEN
        node.onMsg(ctx, msg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(msg);
    }

    @Test
    void givenClearedAlarm_whenOnMsg_then_False() throws JnksIotNodeException {
        // GIVEN
        var alarm = new Alarm();
        alarm.setId(ALARM_ID);
        alarm.setOriginator(DEVICE_ID);
        alarm.setType("General Alarm");
        alarm.setCleared(true);

        String msgData = JacksonUtil.toString(alarm);
        JnksIotMsg msg = getJnksIotMsg(msgData);

        when(alarmService.findAlarmByIdAsync(TENANT_ID, ALARM_ID)).thenReturn(Futures.immediateFuture(alarm));

        // WHEN
        node.onMsg(ctx, msg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(msg);
    }

    @Test
    void givenDeletedAlarm_whenOnMsg_then_Failure() throws JnksIotNodeException {
        // GIVEN
        var alarm = new Alarm();
        alarm.setId(ALARM_ID);
        alarm.setOriginator(DEVICE_ID);
        alarm.setType("General Alarm");
        alarm.setCleared(true);

        String msgData = JacksonUtil.toString(alarm);
        JnksIotMsg msg = getJnksIotMsg(msgData);

        when(alarmService.findAlarmByIdAsync(TENANT_ID, ALARM_ID)).thenReturn(Futures.immediateFuture(null));

        // WHEN
        node.onMsg(ctx, msg);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(ctx, times(1)).tellFailure(newMsgCaptor.capture(), throwableCaptor.capture());
        verify(ctx, never()).tellSuccess(any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(msg);
        Throwable value = throwableCaptor.getValue();
        assertThat(value).isInstanceOf(JnksIotNodeException.class).hasMessage("No such alarm found.");
    }

    @Test
    void givenUnparseableAlarm_whenOnMsg_then_Failure() {
        String msgData = "{\"Number\":1113718,\"id\":8.1}";
        JnksIotMsg msg = getJnksIotMsg(msgData);
        willReturn("Default Rule Chain").given(ctx).getRuleChainName();

        assertThatThrownBy(() -> node.onMsg(ctx, msg))
                .as("onMsg")
                .isInstanceOf(JnksIotNodeException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.IllegalArgumentException: The given string value cannot be transformed to Json object: {\"Number\":1113718,\"id\":8.1}");
    }

    private JnksIotMsg getJnksIotMsg(String msgData) {
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(msgData)
                .build();
    }

}
