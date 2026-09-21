package com.jnks.iot.rule.engine.action;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.ListeningExecutor;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.RuleEngineAlarmService;
import com.jnks.iot.rule.engine.api.ScriptEngine;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmApiCallResult;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.script.ScriptLanguage;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JnksIotClearAlarmNodeTest {

    @Mock
    JnksIotContext ctxMock;
    @Mock
    RuleEngineAlarmService alarmServiceMock;
    @Mock
    ScriptEngine alarmDetailsScriptMock;

    @Captor
    ArgumentCaptor<Runnable> successCaptor;
    @Captor
    ArgumentCaptor<Consumer<Throwable>> failureCaptor;

    JnksIotClearAlarmNode node;

    final TenantId tenantId = TenantId.fromUUID(Uuids.timeBased());
    final EntityId msgOriginator = new DeviceId(Uuids.timeBased());
    final EntityId alarmOriginator = new AlarmId(Uuids.timeBased());
    JnksIotMsgMetaData metadata;

    ListeningExecutor dbExecutor;

    @BeforeEach
    void before() {
        dbExecutor = new TestDbCallbackExecutor();
        metadata = new JnksIotMsgMetaData();
    }

    @Test
    void alarmCanBeCleared() {
        initWithClearAlarmScript();
        metadata.putValue("key", "value");
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(msgOriginator)
                .copyMetaData(metadata)
                .data("{\"temperature\": 50}")
                .build();

        long oldEndDate = System.currentTimeMillis();
        Alarm activeAlarm = Alarm.builder().type("SomeType").tenantId(tenantId).originator(msgOriginator).severity(AlarmSeverity.WARNING).endTs(oldEndDate).build();

        Alarm expectedAlarm = Alarm.builder()
                .tenantId(tenantId)
                .originator(msgOriginator)
                .cleared(true)
                .severity(AlarmSeverity.WARNING)
                .propagate(false)
                .type("SomeType")
                .details(null)
                .endTs(oldEndDate)
                .build();

        when(alarmDetailsScriptMock.executeJsonAsync(msg)).thenReturn(Futures.immediateFuture(null));
        when(alarmServiceMock.findLatestActiveByOriginatorAndType(tenantId, msgOriginator, "SomeType")).thenReturn(activeAlarm);
        when(alarmServiceMock.clearAlarm(eq(activeAlarm.getTenantId()), eq(activeAlarm.getId()), anyLong(), nullable(JsonNode.class)))
                .thenReturn(AlarmApiCallResult.builder()
                        .successful(true)
                        .cleared(true)
                        .alarm(new AlarmInfo(expectedAlarm))
                        .build());

        node.onMsg(ctxMock, msg);

        verify(ctxMock).enqueue(any(), successCaptor.capture(), failureCaptor.capture());
        successCaptor.getValue().run();
        verify(ctxMock).tellNext(any(), eq("Cleared"));

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<JnksIotMsgType> typeCaptor = ArgumentCaptor.forClass(JnksIotMsgType.class);
        ArgumentCaptor<EntityId> originatorCaptor = ArgumentCaptor.forClass(EntityId.class);
        ArgumentCaptor<JnksIotMsgMetaData> metadataCaptor = ArgumentCaptor.forClass(JnksIotMsgMetaData.class);
        ArgumentCaptor<String> dataCaptor = ArgumentCaptor.forClass(String.class);
        verify(ctxMock).transformMsg(msgCaptor.capture(), typeCaptor.capture(), originatorCaptor.capture(), metadataCaptor.capture(), dataCaptor.capture());

        assertThat(JnksIotMsgType.ALARM).isEqualTo(typeCaptor.getValue());
        assertThat(msgOriginator).isEqualTo(originatorCaptor.getValue());
        assertThat("value").isEqualTo(metadataCaptor.getValue().getValue("key"));
        assertThat(Boolean.TRUE.toString()).isEqualTo(metadataCaptor.getValue().getValue(DataConstants.IS_CLEARED_ALARM));
        assertThat(metadata).isNotSameAs(metadataCaptor.getValue());

        Alarm actualAlarm = JacksonUtil.fromBytes(dataCaptor.getValue().getBytes(), Alarm.class);
        assertThat(actualAlarm).isEqualTo(expectedAlarm);
    }

    @Test
    void alarmCanBeClearedWithAlarmOriginator() {
        initWithClearAlarmScript();
        metadata.putValue("key", "value");
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(alarmOriginator)
                .copyMetaData(metadata)
                .data("{\"temperature\": 50}")
                .build();

        long oldEndDate = System.currentTimeMillis();
        AlarmId id = new AlarmId(alarmOriginator.getId());
        Alarm activeAlarm = Alarm.builder().type("SomeType").tenantId(tenantId).originator(msgOriginator).severity(AlarmSeverity.WARNING).endTs(oldEndDate).build();
        activeAlarm.setId(id);

        Alarm expectedAlarm = Alarm.builder()
                .tenantId(tenantId)
                .originator(msgOriginator)
                .cleared(true)
                .severity(AlarmSeverity.WARNING)
                .propagate(false)
                .type("SomeType")
                .details(null)
                .endTs(oldEndDate)
                .build();
        expectedAlarm.setId(id);

        when(alarmDetailsScriptMock.executeJsonAsync(msg)).thenReturn(Futures.immediateFuture(null));
        when(alarmServiceMock.findAlarmById(tenantId, id)).thenReturn(activeAlarm);
        when(alarmServiceMock.clearAlarm(eq(activeAlarm.getTenantId()), eq(activeAlarm.getId()), anyLong(), nullable(JsonNode.class)))
                .thenReturn(AlarmApiCallResult.builder()
                        .successful(true)
                        .cleared(true)
                        .alarm(new AlarmInfo(expectedAlarm))
                        .build());

        node.onMsg(ctxMock, msg);

        verify(ctxMock).enqueue(any(), successCaptor.capture(), failureCaptor.capture());
        successCaptor.getValue().run();
        verify(ctxMock).tellNext(any(), eq("Cleared"));

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<JnksIotMsgType> typeCaptor = ArgumentCaptor.forClass(JnksIotMsgType.class);
        ArgumentCaptor<EntityId> originatorCaptor = ArgumentCaptor.forClass(EntityId.class);
        ArgumentCaptor<JnksIotMsgMetaData> metadataCaptor = ArgumentCaptor.forClass(JnksIotMsgMetaData.class);
        ArgumentCaptor<String> dataCaptor = ArgumentCaptor.forClass(String.class);
        verify(ctxMock).transformMsg(msgCaptor.capture(), typeCaptor.capture(), originatorCaptor.capture(), metadataCaptor.capture(), dataCaptor.capture());

        assertThat(JnksIotMsgType.ALARM).isEqualTo(typeCaptor.getValue());
        assertThat(alarmOriginator).isEqualTo(originatorCaptor.getValue());
        assertThat("value").isEqualTo(metadataCaptor.getValue().getValue("key"));
        assertThat(Boolean.TRUE.toString()).isEqualTo(metadataCaptor.getValue().getValue(DataConstants.IS_CLEARED_ALARM));
        assertThat(metadata).isNotSameAs(metadataCaptor.getValue());

        Alarm actualAlarm = JacksonUtil.fromBytes(dataCaptor.getValue().getBytes(), Alarm.class);
        assertThat(actualAlarm).isEqualTo(expectedAlarm);
    }

    private void initWithClearAlarmScript() {
        try {
            JnksIotClearAlarmNodeConfiguration config = new JnksIotClearAlarmNodeConfiguration();
            config.setAlarmType("SomeType");
            config.setScriptLang(ScriptLanguage.JS);
            config.setAlarmDetailsBuildJs("DETAILS");
            JnksIotNodeConfiguration nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

            when(ctxMock.createScriptEngine(ScriptLanguage.JS, "DETAILS")).thenReturn(alarmDetailsScriptMock);

            when(ctxMock.getTenantId()).thenReturn(tenantId);
            when(ctxMock.getAlarmService()).thenReturn(alarmServiceMock);
            when(ctxMock.getDbCallbackExecutor()).thenReturn(dbExecutor);

            node = new JnksIotClearAlarmNode();
            node.init(ctxMock, nodeConfiguration);
        } catch (JnksIotNodeException ex) {
            throw new IllegalStateException(ex);
        }
    }

}
