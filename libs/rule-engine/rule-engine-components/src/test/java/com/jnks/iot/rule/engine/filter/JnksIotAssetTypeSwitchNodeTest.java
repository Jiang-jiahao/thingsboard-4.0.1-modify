package com.jnks.iot.rule.engine.filter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleEngineAssetProfileCache;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.CustomerId;
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

class JnksIotAssetTypeSwitchNodeTest {

    private AssetId assetId;
    private AssetId assetIdDeleted;
    private JnksIotContext ctx;
    private JnksIotAssetTypeSwitchNode node;
    private JnksIotMsgCallback callback;

    @BeforeEach
    void setUp() throws JnksIotNodeException {
        TenantId tenantId = new TenantId(UUID.randomUUID());
        assetId = new AssetId(UUID.randomUUID());
        assetIdDeleted = new AssetId(UUID.randomUUID());

        AssetProfile assetProfile = new AssetProfile();
        assetProfile.setTenantId(tenantId);
        assetProfile.setName("TestAssetProfile");

        //node
        EmptyNodeConfiguration config = new EmptyNodeConfiguration();
        node = new JnksIotAssetTypeSwitchNode();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        //init mock
        ctx = mock(JnksIotContext.class);
        RuleEngineAssetProfileCache assetProfileCache = mock(RuleEngineAssetProfileCache.class);
        callback = mock(JnksIotMsgCallback.class);

        when(ctx.getTenantId()).thenReturn(tenantId);
        when(ctx.getAssetProfileCache()).thenReturn(assetProfileCache);

        doReturn(assetProfile).when(assetProfileCache).get(tenantId, assetId);
        doReturn(null).when(assetProfileCache).get(tenantId, assetIdDeleted);
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
            node.onMsg(ctx, getJnksIotMsg(assetIdDeleted));
        }).isInstanceOf(JnksIotNodeException.class).hasMessageContaining("Asset profile for entity id");
    }

    @Test
    void givenMsg_whenOnMsg_then_Success() throws JnksIotNodeException {
        JnksIotMsg msg = getJnksIotMsg(assetId);
        node.onMsg(ctx, msg);

        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq("TestAssetProfile"));
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
