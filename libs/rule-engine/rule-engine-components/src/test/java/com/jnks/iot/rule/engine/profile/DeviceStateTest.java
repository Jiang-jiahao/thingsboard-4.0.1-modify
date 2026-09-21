package com.jnks.iot.rule.engine.profile;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleEngineAlarmService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.alarm.AlarmApiCallResult;
import com.jnks.iot.server.common.data.alarm.AlarmCreateOrUpdateActiveRequest;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.common.data.alarm.AlarmSeverity;
import com.jnks.iot.server.common.data.device.profile.AlarmCondition;
import com.jnks.iot.server.common.data.device.profile.AlarmConditionFilter;
import com.jnks.iot.server.common.data.device.profile.AlarmConditionFilterKey;
import com.jnks.iot.server.common.data.device.profile.AlarmConditionKeyType;
import com.jnks.iot.server.common.data.device.profile.AlarmRule;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileAlarm;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.device.profile.SimpleAlarmConditionSpec;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.query.BooleanFilterPredicate;
import com.jnks.iot.server.common.data.query.EntityKeyValueType;
import com.jnks.iot.server.common.data.query.FilterPredicateValue;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.device.DeviceService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeviceStateTest {

    private JnksIotContext ctx;

    @BeforeEach
    public void beforeEach() {
        ctx = mock(JnksIotContext.class);

        when(ctx.getDeviceService()).thenReturn(mock(DeviceService.class));

        AttributesService attributesService = mock(AttributesService.class);
        when(attributesService.find(any(), any(), any(AttributeScope.class), anyCollection())).thenReturn(Futures.immediateFuture(Collections.emptyList()));
        when(ctx.getAttributesService()).thenReturn(attributesService);

        RuleEngineAlarmService alarmService = mock(RuleEngineAlarmService.class);
        when(alarmService.findLatestActiveByOriginatorAndType(any(), any(), any())).thenReturn(null);
        when(alarmService.createAlarm(any())).thenAnswer(invocationOnMock -> {
            AlarmCreateOrUpdateActiveRequest request = invocationOnMock.getArgument(0);
            return AlarmApiCallResult.builder()
                    .successful(true)
                    .created(true)
                    .modified(true)
                    .alarm(new AlarmInfo(new Alarm(new AlarmId(UUID.randomUUID()))))
                    .build();
        });
        when(ctx.getAlarmService()).thenReturn(alarmService);

        when(ctx.newMsg(any(), any(JnksIotMsgType.class), any(), any(), any(), any())).thenAnswer(invocationOnMock -> {
            JnksIotMsgType type = invocationOnMock.getArgument(1);
            String data = invocationOnMock.getArgument(invocationOnMock.getArguments().length - 1);
            return JnksIotMsg.newMsg()
                    .type(type)
                    .copyMetaData(JnksIotMsgMetaData.EMPTY)
                    .data(data)
                    .build();
        });

    }

    @Test
    public void whenAttributeIsDeleted_thenUnneededAlarmRulesAreNotReevaluated() throws Exception {

        DeviceProfileAlarm alarmConfig = createAlarmConfigWithBoolAttrCondition("enabled", false);
        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        DeviceState deviceState = createDeviceState(deviceId, alarmConfig);

        JnksIotMsg attributeUpdateMsg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(deviceId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data("{ \"enabled\": false }")
                .build();

        deviceState.process(ctx, attributeUpdateMsg);

        ArgumentCaptor<JnksIotMsg> resultMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).enqueueForTellNext(resultMsgCaptor.capture(), eq("Alarm Created"));
        Alarm alarm = JacksonUtil.fromString(resultMsgCaptor.getValue().getData(), Alarm.class);

        deviceState.process(ctx, JnksIotMsg.newMsg()
                .type(JnksIotMsgType.ALARM_CLEAR)
                .originator(deviceId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.toString(alarm))
                .build());
        reset(ctx);

        String deletedAttributes = "{ \"attributes\": [ \"other\" ] }";
        deviceState.process(ctx, JnksIotMsg.newMsg()
                .type(JnksIotMsgType.ATTRIBUTES_DELETED)
                .originator(deviceId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(deletedAttributes)
                .build());
        verify(ctx, never()).enqueueForTellNext(any(), anyString());
    }

    @Test
    public void whenDeletingClearedAlarm_thenNoError() throws Exception {
        DeviceProfileAlarm alarmConfig = createAlarmConfigWithBoolAttrCondition("enabled", false);
        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        DeviceState deviceState = createDeviceState(deviceId, alarmConfig);

        JnksIotMsg attributeUpdateMsg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(deviceId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data("{ \"enabled\": false }")
                .build();

        deviceState.process(ctx, attributeUpdateMsg);
        ArgumentCaptor<JnksIotMsg> resultMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx).enqueueForTellNext(resultMsgCaptor.capture(), eq("Alarm Created"));
        Alarm alarm = JacksonUtil.fromString(resultMsgCaptor.getValue().getData(), Alarm.class);

        deviceState.process(ctx, JnksIotMsg.newMsg()
                .type(JnksIotMsgType.ALARM_CLEAR)
                .originator(deviceId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.toString(alarm))
                .build());

        JnksIotMsg alarmDeleteNotification = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.ALARM_DELETE)
                .originator(deviceId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JacksonUtil.toString(alarm))
                .build();
        assertDoesNotThrow(() -> {
            deviceState.process(ctx, alarmDeleteNotification);
        });
    }


    private DeviceState createDeviceState(DeviceId deviceId, DeviceProfileAlarm... alarmConfigs) {
        DeviceProfile deviceProfile = new DeviceProfile();
        DeviceProfileData profileData = new DeviceProfileData();
        profileData.setAlarms(List.of(alarmConfigs));
        deviceProfile.setProfileData(profileData);

        ProfileState profileState = new ProfileState(deviceProfile);
        return new DeviceState(ctx, new JnksIotDeviceProfileNodeConfiguration(),
                deviceId, profileState, null);
    }

    private DeviceProfileAlarm createAlarmConfigWithBoolAttrCondition(String key, boolean value) {

        AlarmConditionFilter condition = new AlarmConditionFilter();
        condition.setKey(new AlarmConditionFilterKey(AlarmConditionKeyType.ATTRIBUTE, key));
        condition.setValueType(EntityKeyValueType.BOOLEAN);
        BooleanFilterPredicate predicate = new BooleanFilterPredicate();
        predicate.setOperation(BooleanFilterPredicate.BooleanOperation.EQUAL);
        predicate.setValue(new FilterPredicateValue<>(value));
        condition.setPredicate(predicate);

        DeviceProfileAlarm alarmConfig = new DeviceProfileAlarm();
        alarmConfig.setId("MyAlarmID");
        alarmConfig.setAlarmType("MyAlarm");
        AlarmRule alarmRule = new AlarmRule();
        AlarmCondition alarmCondition = new AlarmCondition();
        alarmCondition.setSpec(new SimpleAlarmConditionSpec());
        alarmCondition.setCondition(List.of(condition));
        alarmRule.setCondition(alarmCondition);
        alarmConfig.setCreateRules(new TreeMap<>(Map.of(AlarmSeverity.CRITICAL, alarmRule)));

        return alarmConfig;
    }

}
