package com.jnks.iot.rule.engine.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationTypeGroup;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.relation.RelationService;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JnksIotCheckRelationNodeTest extends AbstractRuleNodeUpgradeTest {

    private final TenantId TENANT_ID = new TenantId(UUID.randomUUID());
    private final DeviceId ORIGINATOR_ID = new DeviceId(UUID.randomUUID());
    private final TestDbCallbackExecutor DB_EXECUTOR = new TestDbCallbackExecutor();
    private final JnksIotMsg EMPTY_POST_ATTRIBUTES_MSG = JnksIotMsg.newMsg()
            .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
            .originator(ORIGINATOR_ID)
            .copyMetaData(JnksIotMsgMetaData.EMPTY)
            .data(JnksIotMsg.EMPTY_JSON_OBJECT)
            .build();

    private JnksIotCheckRelationNode node;

    private JnksIotContext ctx;
    private RelationService relationService;

    @BeforeEach
    void setUp() {
        ctx = mock(JnksIotContext.class);
        relationService = mock(RelationService.class);

        when(ctx.getTenantId()).thenReturn(TENANT_ID);
        when(ctx.getRelationService()).thenReturn(relationService);
        when(ctx.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        node = spy(new JnksIotCheckRelationNode());
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenDefaultConfig_whenInit_then_throwException() {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config))));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("Entity should be specified!");
    }

    @Test
    void givenCustomConfigWithCheckRelationToSpecificEntity_whenOnMsg_then_True() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();

        AssetId assetId = new AssetId(UUID.randomUUID());
        config.setEntityType(assetId.getEntityType().name());
        config.setEntityId(assetId.getId().toString());

        when(relationService.checkRelationAsync(TENANT_ID, ORIGINATOR_ID, assetId, config.getRelationType(), RelationTypeGroup.COMMON)).thenReturn(Futures.immediateFuture(true));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfigWithCheckRelationToSpecificEntity_whenOnMsg_then_False() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();

        AssetId assetId = new AssetId(UUID.randomUUID());
        config.setEntityType(assetId.getEntityType().name());
        config.setEntityId(assetId.getId().toString());

        when(relationService.checkRelationAsync(TENANT_ID, ORIGINATOR_ID, assetId, config.getRelationType(), RelationTypeGroup.COMMON)).thenReturn(Futures.immediateFuture(false));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfigWithCheckRelationToSpecificEntityAndDirectionTo_whenOnMsg_then_True() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();

        AssetId assetId = new AssetId(UUID.randomUUID());
        config.setEntityType(assetId.getEntityType().name());
        config.setEntityId(assetId.getId().toString());
        config.setDirection(EntitySearchDirection.TO.name());

        when(relationService.checkRelationAsync(TENANT_ID, assetId, ORIGINATOR_ID, config.getRelationType(), RelationTypeGroup.COMMON)).thenReturn(Futures.immediateFuture(true));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfigWithCheckRelationToSpecificEntityAndDirectionTo_whenOnMsg_then_False() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();

        AssetId assetId = new AssetId(UUID.randomUUID());
        config.setEntityType(assetId.getEntityType().name());
        config.setEntityId(assetId.getId().toString());
        config.setDirection(EntitySearchDirection.TO.name());

        when(relationService.checkRelationAsync(TENANT_ID, assetId, ORIGINATOR_ID, config.getRelationType(), RelationTypeGroup.COMMON)).thenReturn(Futures.immediateFuture(false));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfig_whenOnMsg_then_True() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();
        config.setCheckForSingleEntity(false);
        var entityRelation = new EntityRelation();
        entityRelation.setFrom(ORIGINATOR_ID);
        entityRelation.setTo(new AssetId(UUID.randomUUID()));
        entityRelation.setType(EntityRelation.CONTAINS_TYPE);
        entityRelation.setTypeGroup(RelationTypeGroup.COMMON);

        when(relationService.findByFromAndTypeAsync(TENANT_ID, ORIGINATOR_ID, config.getRelationType(), RelationTypeGroup.COMMON)).thenReturn(Futures.immediateFuture(List.of(entityRelation)));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        verify(relationService, never()).findByToAndTypeAsync(any(), any(), anyString(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfig_whenOnMsg_then_False() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();
        config.setCheckForSingleEntity(false);

        when(relationService.findByFromAndTypeAsync(TENANT_ID, ORIGINATOR_ID, config.getRelationType(), RelationTypeGroup.COMMON)).thenReturn(Futures.immediateFuture(Collections.emptyList()));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        verify(relationService, never()).findByToAndTypeAsync(any(), any(), anyString(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfigDirectionTo_whenOnMsg_then_True() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();
        config.setCheckForSingleEntity(false);
        config.setDirection(EntitySearchDirection.TO.name());
        var entityRelation = new EntityRelation();
        entityRelation.setFrom(new AssetId(UUID.randomUUID()));
        entityRelation.setTo(ORIGINATOR_ID);
        entityRelation.setType(EntityRelation.CONTAINS_TYPE);
        entityRelation.setTypeGroup(RelationTypeGroup.COMMON);

        when(relationService.findByToAndTypeAsync(TENANT_ID, ORIGINATOR_ID, config.getRelationType(), RelationTypeGroup.COMMON)).thenReturn(Futures.immediateFuture(List.of(entityRelation)));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.TRUE));
        verify(ctx, never()).tellFailure(any(), any());
        verify(relationService, never()).findByFromAndTypeAsync(any(), any(), anyString(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenCustomConfigDirectionTo_whenOnMsg_then_False() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();
        config.setCheckForSingleEntity(false);
        config.setDirection(EntitySearchDirection.TO.name());

        when(relationService.findByToAndTypeAsync(TENANT_ID, ORIGINATOR_ID, config.getRelationType(), RelationTypeGroup.COMMON)).thenReturn(Futures.immediateFuture(Collections.emptyList()));
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // WHEN
        node.onMsg(ctx, EMPTY_POST_ATTRIBUTES_MSG);

        // THEN
        ArgumentCaptor<JnksIotMsg> newMsgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctx, times(1)).tellNext(newMsgCaptor.capture(), eq(JnksIotNodeConnectionType.FALSE));
        verify(ctx, never()).tellFailure(any(), any());
        verify(relationService, never()).findByFromAndTypeAsync(any(), any(), anyString(), any());
        JnksIotMsg newMsg = newMsgCaptor.getValue();
        assertThat(newMsg).isNotNull();
        assertThat(newMsg).isSameAs(EMPTY_POST_ATTRIBUTES_MSG);
    }

    @Test
    void givenOldConfig_whenUpgrade_thenShouldReturnTrueResultWithNewConfig() throws Exception {
        // GIVEN
        var config = new JnksIotCheckRelationNodeConfiguration().defaultConfiguration();
        config.setEntityType(ORIGINATOR_ID.getEntityType().name());
        config.setEntityId(ORIGINATOR_ID.getId().toString());
        String oldConfig = "{\"checkForSingleEntity\":true,\"direction\":\"TO\",\"entityType\":\"" + config.getEntityType() + "\",\"entityId\":\"" + config.getEntityId() + "\",\"relationType\":\"Contains\"}";
        JsonNode configJson = JacksonUtil.toJsonNode(oldConfig);
        // WHEN
        JnksIotPair<Boolean, JsonNode> upgrade = node.upgrade(0, configJson);
        // THEN
        assertTrue(upgrade.getFirst());
        assertEquals(config, JacksonUtil.treeToValue(upgrade.getSecond(), config.getClass()));
    }

    // Rule nodes upgrade
    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                // version 0 config, FROM direction.
                Arguments.of(0,
                        "{\"checkForSingleEntity\":true,\"direction\":\"FROM\",\"entityType\":\"DEVICE\",\"entityId\":\"1943b1eb-2811-4373-846d-6ca2f527bf9e\",\"relationType\":\"Contains\"}",
                        true,
                        "{\"checkForSingleEntity\":true,\"direction\":\"TO\",\"entityType\":\"DEVICE\",\"entityId\":\"1943b1eb-2811-4373-846d-6ca2f527bf9e\",\"relationType\":\"Contains\"}"),
                // version 0 config, TO direction.
                Arguments.of(0,
                        "{\"checkForSingleEntity\":true,\"direction\":\"TO\",\"entityType\":\"DEVICE\",\"entityId\":\"1943b1eb-2811-4373-846d-6ca2f527bf9e\",\"relationType\":\"Contains\"}",
                        true,
                        "{\"checkForSingleEntity\":true,\"direction\":\"FROM\",\"entityType\":\"DEVICE\",\"entityId\":\"1943b1eb-2811-4373-846d-6ca2f527bf9e\",\"relationType\":\"Contains\"}")
        );
    }

    @Override
    protected JnksIotNode getTestNode() {
        return node;
    }
}
