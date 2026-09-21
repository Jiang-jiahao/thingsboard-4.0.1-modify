package com.jnks.iot.rule.engine.filter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleEngineDeviceProfileCache;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JnksIotDeviceTypeSwitchNodeTest {

    private DeviceId deviceId;
    private DeviceId deviceIdDeleted;
    private JnksIotContext ctx;
    private JnksIotDeviceTypeSwitchNode node;
    private JnksIotMsgCallback callback;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        TenantId tenantId = new TenantId(UUID.randomUUID());
        deviceId = new DeviceId(UUID.randomUUID());
        deviceIdDeleted = new DeviceId(UUID.randomUUID());

        DeviceProfile deviceProfile = new DeviceProfile();
        deviceProfile.setTenantId(tenantId);
        deviceProfile.setName("TestDeviceProfile");

        //node
        EmptyNodeConfiguration config = new EmptyNodeConfiguration();
        node = new JnksIotDeviceTypeSwitchNode();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        //init mock
        ctx = mock(JnksIotContext.class);
        RuleEngineDeviceProfileCache deviceProfileCache = mock(RuleEngineDeviceProfileCache.class);
        callback = mock(JnksIotMsgCallback.class);

        when(ctx.getTenantId()).thenReturn(tenantId);
        when(ctx.getDeviceProfileCache()).thenReturn(deviceProfileCache);

        doReturn(deviceProfile).when(deviceProfileCache).get(tenantId, deviceId);
        doReturn(null).when(deviceProfileCache).get(tenantId, deviceIdDeleted);
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenMsg_whenOnMsg_then_Fail() {
        CustomerId customerId = new CustomerId(UUID.randomUUID());
        assertThatThrownBy(() -> {
            node.onMsg(ctx, getJnksIotMsg(customerId));
        }).isInstanceOf(JnksIotNodeException.class).hasMessageContaining("Unsupported originator type");
    }

    @Test
    void givenMsg_whenOnMsg_EntityIdDeleted_then_Fail() {
        assertThatThrownBy(() -> {
            node.onMsg(ctx, getJnksIotMsg(deviceIdDeleted));
        }).isInstanceOf(JnksIotNodeException.class).hasMessageContaining("Device profile for entity id");
    }

    @Test
    void givenMsg_whenOnMsg_then_Success() throws JnksIotNodeException {
        JnksIotMsg msg = getJnksIotMsg(deviceId);
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq("TestDeviceProfile"));
        verify(ctx, never()).tellFailure(any(), any());

        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(msg);
    }

    private JnksIotMsg getJnksIotMsg(EntityId entityId) {
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(entityId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .callback(callback)
                .build();
    }
}
