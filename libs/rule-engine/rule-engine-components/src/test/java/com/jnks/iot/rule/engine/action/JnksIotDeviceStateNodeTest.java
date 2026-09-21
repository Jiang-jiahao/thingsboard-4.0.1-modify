package com.jnks.iot.rule.engine.action;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.DeviceStateManager;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.common.msg.queue.PartitionChangeMsg;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;
import com.jnks.iot.server.common.msg.tools.JnksIotRateLimits;

import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
public class JnksIotDeviceStateNodeTest {

    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private DeviceStateManager deviceStateManagerMock;
    @Captor
    private ArgumentCaptor<JnksIotCallback> callbackCaptor;
    private JnksIotDeviceStateNode node;
    private JnksIotDeviceStateNodeConfiguration config;

    private static final TenantId TENANT_ID = TenantId.fromUUID(UUID.randomUUID());
    private static final DeviceId DEVICE_ID = new DeviceId(UUID.randomUUID());
    private static final long METADATA_TS = 123L;
    private JnksIotMsg msg;

    @BeforeEach
    public void setup() {
        var metaData = new JnksIotMsgMetaData();
        metaData.putValue("deviceName", "My humidity sensor");
        metaData.putValue("deviceType", "Humidity sensor");
        metaData.putValue("ts", String.valueOf(METADATA_TS));
        var data = JacksonUtil.newObjectNode();
        data.put("humidity", 58.3);
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data(JacksonUtil.toString(data))
                .build();
    }

    @BeforeEach
    public void setUp() {
        node = new JnksIotDeviceStateNode();
        config = new JnksIotDeviceStateNodeConfiguration().defaultConfiguration();
    }

    @Test
    public void givenDefaultConfiguration_whenInvoked_thenCorrectValuesAreSet() {
        assertThat(config.getEvent()).isEqualTo(JnksIotMsgType.ACTIVITY_EVENT);
    }

    @Test
    public void givenNullEventInConfig_whenInit_thenThrowsUnrecoverableJnksIotNodeException() {
        // GIVEN-WHEN-THEN
        assertThatThrownBy(() -> initNode(null))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("Event cannot be null!")
                .matches(e -> ((JnksIotNodeException) e).isUnrecoverable());
    }

    @Test
    public void givenInvalidRateLimitConfig_whenInit_thenUsesDefaultConfig() {
        // GIVEN
        given(ctxMock.getDeviceStateNodeRateLimitConfig()).willReturn("invalid rate limit config");
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.getSelfId()).willReturn(new RuleNodeId(UUID.randomUUID()));

        // WHEN
        try {
            initNode(JnksIotMsgType.ACTIVITY_EVENT);
        } catch (Exception e) {
            fail("Node failed to initialize!", e);
        }

        // THEN
        String actualRateLimitConfig = (String) ReflectionTestUtils.getField(node, "rateLimitConfig");
        assertThat(actualRateLimitConfig).isEqualTo("1:1,30:60,60:3600");
    }

    @Test
    public void givenMsgArrivedTooFast_whenOnMsg_thenRateLimitsThisMsg() {
        // GIVEN
        ConcurrentReferenceHashMap<DeviceId, JnksIotRateLimits> rateLimits = new ConcurrentReferenceHashMap<>();
        ReflectionTestUtils.setField(node, "rateLimits", rateLimits);

        var rateLimitMock = mock(JnksIotRateLimits.class);
        rateLimits.put(DEVICE_ID, rateLimitMock);

        given(rateLimitMock.tryConsume()).willReturn(false);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        then(ctxMock).should().tellNext(msg, "Rate limited");
        then(ctxMock).should(never()).tellSuccess(any());
        then(ctxMock).should(never()).tellFailure(any(), any());
        then(ctxMock).shouldHaveNoMoreInteractions();
        then(deviceStateManagerMock).shouldHaveNoInteractions();
    }

    @Test
    public void givenHasNonLocalDevices_whenOnPartitionChange_thenRemovesEntriesForNonLocalDevices() {
        // GIVEN
        ConcurrentReferenceHashMap<DeviceId, JnksIotRateLimits> rateLimits = new ConcurrentReferenceHashMap<>();
        ReflectionTestUtils.setField(node, "rateLimits", rateLimits);

        rateLimits.put(DEVICE_ID, new JnksIotRateLimits("1:1"));
        given(ctxMock.isLocalEntity(eq(DEVICE_ID))).willReturn(true);

        DeviceId nonLocalDeviceId1 = new DeviceId(UUID.randomUUID());
        rateLimits.put(nonLocalDeviceId1, new JnksIotRateLimits("2:2"));
        given(ctxMock.isLocalEntity(eq(nonLocalDeviceId1))).willReturn(false);

        DeviceId nonLocalDeviceId2 = new DeviceId(UUID.randomUUID());
        rateLimits.put(nonLocalDeviceId2, new JnksIotRateLimits("3:3"));
        given(ctxMock.isLocalEntity(eq(nonLocalDeviceId2))).willReturn(false);

        // WHEN
        node.onPartitionChangeMsg(ctxMock, new PartitionChangeMsg(ServiceType.JNKS_IOT_RULE_ENGINE));

        // THEN
        assertThat(rateLimits)
                .containsKey(DEVICE_ID)
                .doesNotContainKey(nonLocalDeviceId1)
                .doesNotContainKey(nonLocalDeviceId2)
                .size().isOne();
    }

    @ParameterizedTest
    @EnumSource(
            value = JnksIotMsgType.class,
            names = {"CONNECT_EVENT", "ACTIVITY_EVENT", "DISCONNECT_EVENT", "INACTIVITY_EVENT"},
            mode = EnumSource.Mode.EXCLUDE
    )
    public void givenUnsupportedEventInConfig_whenInit_thenThrowsUnrecoverableJnksIotNodeException(JnksIotMsgType unsupportedEvent) {
        // GIVEN-WHEN-THEN
        assertThatThrownBy(() -> initNode(unsupportedEvent))
                .isInstanceOf(JnksIotNodeException.class)
                .hasMessage("Unsupported event: " + unsupportedEvent)
                .matches(e -> ((JnksIotNodeException) e).isUnrecoverable());
    }

    @ParameterizedTest
    @EnumSource(value = EntityType.class, names = "DEVICE", mode = EnumSource.Mode.EXCLUDE)
    public void givenNonDeviceOriginator_whenOnMsg_thenTellsSuccessAndNoActivityActionsTriggered(EntityType unsupportedType) {
        // GIVEN
        var nonDeviceOriginator = new EntityId() {

            @Override
            public UUID getId() {
                return UUID.randomUUID();
            }

            @Override
            public EntityType getEntityType() {
                return unsupportedType;
            }
        };
        var msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.ENTITY_CREATED)
                .originator(nonDeviceOriginator)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        then(ctxMock).should().tellFailure(eq(msg), exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported originator entity type: [" + unsupportedType + "]. Only DEVICE entity type is supported.");

        then(ctxMock).shouldHaveNoMoreInteractions();
    }

    @Test
    public void givenMetadataDoesNotContainTs_whenOnMsg_thenMsgTsIsUsedAsEventTs() {
        // GIVEN
        given(ctxMock.getDeviceStateNodeRateLimitConfig()).willReturn("1:1");
        try {
            initNode(JnksIotMsgType.ACTIVITY_EVENT);
        } catch (JnksIotNodeException e) {
            fail("Node failed to initialize!", e);
        }

        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.getDeviceStateManager()).willReturn(deviceStateManagerMock);

        long msgTs = METADATA_TS + 1;
        msg = JnksIotMsg.newMsg()
                .ts(msgTs)
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        then(deviceStateManagerMock).should().onDeviceActivity(eq(TENANT_ID), eq(DEVICE_ID), eq(msgTs), any());
    }

    @ParameterizedTest
    @MethodSource
    public void givenSupportedEventAndDeviceOriginator_whenOnMsg_thenCorrectEventIsSentWithCorrectCallback(JnksIotMsgType supportedEventType, BiConsumer<DeviceStateManager, ArgumentCaptor<JnksIotCallback>> actionVerification) {
        // GIVEN
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.getDeviceStateNodeRateLimitConfig()).willReturn("1:1");
        given(ctxMock.getDeviceStateManager()).willReturn(deviceStateManagerMock);

        try {
            initNode(supportedEventType);
        } catch (JnksIotNodeException e) {
            fail("Node failed to initialize!", e);
        }

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        actionVerification.accept(this.deviceStateManagerMock, this.callbackCaptor);

        JnksIotCallback actualCallback = callbackCaptor.getValue();

        actualCallback.onSuccess();
        then(ctxMock).should().tellSuccess(msg);

        var throwable = new Throwable();
        actualCallback.onFailure(throwable);
        then(ctxMock).should().tellFailure(msg, throwable);


        then(deviceStateManagerMock).shouldHaveNoMoreInteractions();
        then(ctxMock).shouldHaveNoMoreInteractions();
    }

    private static Stream<Arguments> givenSupportedEventAndDeviceOriginator_whenOnMsg_thenCorrectEventIsSentWithCorrectCallback() {
        return Stream.of(
                Arguments.of(JnksIotMsgType.CONNECT_EVENT, (BiConsumer<DeviceStateManager, ArgumentCaptor<JnksIotCallback>>) (deviceStateManagerMock, callbackCaptor) -> then(deviceStateManagerMock).should().onDeviceConnect(eq(TENANT_ID), eq(DEVICE_ID), eq(METADATA_TS), callbackCaptor.capture())),
                Arguments.of(JnksIotMsgType.ACTIVITY_EVENT, (BiConsumer<DeviceStateManager, ArgumentCaptor<JnksIotCallback>>) (deviceStateManagerMock, callbackCaptor) -> then(deviceStateManagerMock).should().onDeviceActivity(eq(TENANT_ID), eq(DEVICE_ID), eq(METADATA_TS), callbackCaptor.capture())),
                Arguments.of(JnksIotMsgType.DISCONNECT_EVENT, (BiConsumer<DeviceStateManager, ArgumentCaptor<JnksIotCallback>>) (deviceStateManagerMock, callbackCaptor) -> then(deviceStateManagerMock).should().onDeviceDisconnect(eq(TENANT_ID), eq(DEVICE_ID), eq(METADATA_TS), callbackCaptor.capture())),
                Arguments.of(JnksIotMsgType.INACTIVITY_EVENT, (BiConsumer<DeviceStateManager, ArgumentCaptor<JnksIotCallback>>) (deviceStateManagerMock, callbackCaptor) -> then(deviceStateManagerMock).should().onDeviceInactivity(eq(TENANT_ID), eq(DEVICE_ID), eq(METADATA_TS), callbackCaptor.capture()))
        );
    }

    private void initNode(JnksIotMsgType event) throws JnksIotNodeException {
        config.setEvent(event);
        var nodeConfig = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        node.init(ctxMock, nodeConfig);
    }

}
