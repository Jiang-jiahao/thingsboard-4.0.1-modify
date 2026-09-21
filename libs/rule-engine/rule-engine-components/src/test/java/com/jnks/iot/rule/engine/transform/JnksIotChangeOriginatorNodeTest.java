package com.jnks.iot.rule.engine.transform;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.ListeningExecutor;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.RuleEngineAlarmService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.data.RelationsQuery;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.alarm.Alarm;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.id.AlarmId;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntityRelationsQuery;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationEntityTypeFilter;
import com.jnks.iot.server.common.data.relation.RelationsSearchParameters;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.asset.AssetService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.dao.relation.RelationService;

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static com.jnks.iot.rule.engine.transform.OriginatorSource.ALARM_ORIGINATOR;
import static com.jnks.iot.rule.engine.transform.OriginatorSource.CUSTOMER;
import static com.jnks.iot.rule.engine.transform.OriginatorSource.ENTITY;
import static com.jnks.iot.rule.engine.transform.OriginatorSource.RELATED;
import static com.jnks.iot.rule.engine.transform.OriginatorSource.TENANT;

@ExtendWith(MockitoExtension.class)
public class JnksIotChangeOriginatorNodeTest {

    private final TenantId TENANT_ID = TenantId.fromUUID(UUID.fromString("79830b6d-4f93-49bd-9b5b-d31ce51da77b"));
    private final CustomerId CUSTOMER_ID = new CustomerId(UUID.fromString("c6b2c94b-5517-4f20-bf8e-ae9407eb8a7a"));
    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("990605a4-db46-4ed4-942f-e18200453571"));
    private final AssetId ASSET_ID = new AssetId(UUID.fromString("55de3f10-1b55-4950-b711-ed132896b260"));

    private final ListeningExecutor dbExecutor = new TestDbCallbackExecutor();

    private JnksIotChangeOriginatorNode node;
    private JnksIotChangeOriginatorNodeConfiguration config;

    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private AssetService assetServiceMock;
    @Mock
    private DeviceService deviceServiceMock;
    @Mock
    private RelationService relationServiceMock;
    @Mock
    private RuleEngineAlarmService alarmServiceMock;

    @BeforeEach
    public void before() throws JnksIotNodeException {
        node = new JnksIotChangeOriginatorNode();
        config = new JnksIotChangeOriginatorNodeConfiguration().defaultConfiguration();
    }

    @Test
    public void verifyDefaultConfig() {
        var config = new JnksIotChangeOriginatorNodeConfiguration().defaultConfiguration();
        assertThat(config.getOriginatorSource()).isEqualTo(CUSTOMER);
        RelationsQuery relationsQuery = new RelationsQuery();
        relationsQuery.setDirection(EntitySearchDirection.FROM);
        relationsQuery.setMaxLevel(1);
        RelationEntityTypeFilter relationEntityTypeFilter = new RelationEntityTypeFilter(EntityRelation.CONTAINS_TYPE, Collections.emptyList());
        relationsQuery.setFilters(Collections.singletonList(relationEntityTypeFilter));
        assertThat(config.getRelationsQuery()).isEqualTo(relationsQuery);
        assertThat(config.getEntityType()).isNull();
        assertThat(config.getEntityNamePattern()).isNull();
    }

    @Test
    public void givenRelatedSourceIsNull_whenInit_thenThrowsException() {
        config.setOriginatorSource(null);

        assertThatThrownBy(() -> node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Originator source should be specified.");
    }

    @Test
    public void givenRelatedSourceAndRelatedQueryIsNull_whenInit_thenThrowsException() {
        config.setOriginatorSource(RELATED);
        config.setRelationsQuery(null);

        assertThatThrownBy(() -> node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Relations query should be specified if 'Related entity' source is selected.");
    }

    @Test
    public void givenEntitySourceAndEntityTypeIsNull_whenInit_thenThrowsException() {
        config.setOriginatorSource(ENTITY);
        config.setEntityType(null);

        assertThatThrownBy(() -> node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Entity type should be specified if 'Entity by name pattern' source is selected.");
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void givenEntitySourceAndEntityNamePatternIsEmpty_whenInit_thenThrowsException(String entityName) {
        config.setOriginatorSource(ENTITY);
        config.setEntityType(EntityType.DEVICE.name());
        config.setEntityNamePattern(entityName);

        assertThatThrownBy(() -> node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Name pattern should be specified if 'Entity by name pattern' source is selected.");
    }

    @Test
    public void givenEntitySourceAndUnexpectedEntityType_whenInit_thenThrowsException() {
        config.setOriginatorSource(ENTITY);
        config.setEntityType(EntityType.TENANT.name());
        config.setEntityNamePattern("tenant-A");

        assertThatThrownBy(() -> node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected entity type TENANT");
    }

    @Test
    public void givenOriginatorSourceIsCustomer_whenOnMsg_thenTellSuccess() throws JnksIotNodeException {
        Device device = new Device(DEVICE_ID);
        device.setCustomerId(CUSTOMER_ID);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        JnksIotMsg expectedMsg = msg.transform()
                .originator(CUSTOMER_ID)
                .build();

        given(ctxMock.getDbCallbackExecutor()).willReturn(dbExecutor);
        given(ctxMock.getDeviceService()).willReturn(deviceServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(deviceServiceMock.findDeviceById(any(TenantId.class), any(DeviceId.class))).willReturn(device);
        given(ctxMock.transformMsgOriginator(any(JnksIotMsg.class), any(EntityId.class))).willReturn(expectedMsg);

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        node.onMsg(ctxMock, msg);

        then(deviceServiceMock).should().findDeviceById(TENANT_ID, DEVICE_ID);
        then(ctxMock).should().transformMsgOriginator(msg, CUSTOMER_ID);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellSuccess(actualMsg.capture());
        assertThat(actualMsg.getValue()).usingRecursiveComparison().ignoringFields("ctx").isEqualTo(expectedMsg);
    }

    @Test
    public void givenOriginatorSourceIsTenant_whenOnMsg_thenTellSuccess() throws JnksIotNodeException {
        config.setOriginatorSource(TENANT);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(ASSET_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        JnksIotMsg expectedMsg = msg.transform()
                .originator(TENANT_ID)
                .build();

        given(ctxMock.getDbCallbackExecutor()).willReturn(dbExecutor);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(ctxMock.transformMsgOriginator(any(JnksIotMsg.class), any(EntityId.class))).willReturn(expectedMsg);

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        node.onMsg(ctxMock, msg);

        then(ctxMock).should().transformMsgOriginator(msg, TENANT_ID);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellSuccess(actualMsg.capture());
        assertThat(actualMsg.getValue()).usingRecursiveComparison().ignoringFields("ctx").isEqualTo(expectedMsg);
    }

    @Test
    public void givenOriginatorSourceIsRelatedAndNewOriginatorIsNull_whenOnMsg_thenTellFailure() throws JnksIotNodeException {
        config.setOriginatorSource(RELATED);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(ASSET_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();

        given(ctxMock.getDbCallbackExecutor()).willReturn(dbExecutor);
        given(ctxMock.getRelationService()).willReturn(relationServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(relationServiceMock.findByQuery(any(TenantId.class), any(EntityRelationsQuery.class))).willReturn(Futures.immediateFuture(Collections.emptyList()));

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        node.onMsg(ctxMock, msg);

        var query = new EntityRelationsQuery();
        var relationsQuery = config.getRelationsQuery();
        var parameters = new RelationsSearchParameters(
                ASSET_ID,
                relationsQuery.getDirection(),
                relationsQuery.getMaxLevel(),
                relationsQuery.isFetchLastLevelOnly()
        );
        query.setParameters(parameters);
        query.setFilters(relationsQuery.getFilters());
        then(relationServiceMock).should().findByQuery(TENANT_ID, query);
        ArgumentCaptor<Throwable> throwable = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(eq(msg), throwable.capture());
        assertThat(throwable.getValue()).isInstanceOf(NoSuchElementException.class).hasMessage("Failed to find new originator!");
    }

    @Test
    public void givenOriginatorSourceIsAlarmOriginator_whenOnMsg_thenTellSuccess() throws JnksIotNodeException {
        config.setOriginatorSource(ALARM_ORIGINATOR);

        AlarmId alarmId = new AlarmId(UUID.fromString("6b43f694-cb5f-4199-9023-e9e40eeb82dd"));
        Alarm alarm = new Alarm(alarmId);
        alarm.setOriginator(DEVICE_ID);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.ALARM)
                .originator(alarmId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();
        JnksIotMsg expectedMsg = msg.transform()
                .originator(DEVICE_ID)
                .build();

        given(ctxMock.getDbCallbackExecutor()).willReturn(dbExecutor);
        given(ctxMock.getAlarmService()).willReturn(alarmServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(alarmServiceMock.findAlarmByIdAsync(any(TenantId.class), any(AlarmId.class))).willReturn(Futures.immediateFuture(alarm));
        given(ctxMock.transformMsgOriginator(any(JnksIotMsg.class), any(EntityId.class))).willReturn(expectedMsg);

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        node.onMsg(ctxMock, msg);

        then(alarmServiceMock).should().findAlarmByIdAsync(TENANT_ID, alarmId);
        then(ctxMock).should().transformMsgOriginator(msg, DEVICE_ID);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellSuccess(actualMsg.capture());
        assertThat(actualMsg.getValue()).usingRecursiveComparison().ignoringFields("ctx").isEqualTo(expectedMsg);
    }

    @ParameterizedTest
    @MethodSource
    public void givenOriginatorSourceIsEntity_whenOnMsg_thenTellSuccess(String entityNamePattern, JnksIotMsgMetaData metaData, String data) throws JnksIotNodeException {
        config.setOriginatorSource(ENTITY);
        config.setEntityType(EntityType.ASSET.name());
        config.setEntityNamePattern(entityNamePattern);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data(data)
                .build();
        JnksIotMsg expectedMsg = msg.transform()
                .originator(ASSET_ID)
                .build();

        given(ctxMock.getDbCallbackExecutor()).willReturn(dbExecutor);
        given(ctxMock.getAssetService()).willReturn(assetServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(assetServiceMock.findAssetByTenantIdAndName(any(TenantId.class), any(String.class))).willReturn(new Asset(ASSET_ID));
        given(ctxMock.transformMsgOriginator(any(JnksIotMsg.class), any(EntityId.class))).willReturn(expectedMsg);

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        node.onMsg(ctxMock, msg);

        String expectedEntityName = JnksIotNodeUtils.processPattern(entityNamePattern, msg);
        then(assetServiceMock).should().findAssetByTenantIdAndName(TENANT_ID, expectedEntityName);
        then(ctxMock).should().transformMsgOriginator(msg, ASSET_ID);
        ArgumentCaptor<JnksIotMsg> actualMsg = ArgumentCaptor.forClass(JnksIotMsg.class);
        then(ctxMock).should().tellSuccess(actualMsg.capture());
        assertThat(actualMsg.getValue()).usingRecursiveComparison().ignoringFields("ctx").isEqualTo(expectedMsg);
    }

    private static Stream<Arguments> givenOriginatorSourceIsEntity_whenOnMsg_thenTellSuccess() {
        return Stream.of(
                Arguments.of("test-asset", JnksIotMsgMetaData.EMPTY, JnksIotMsg.EMPTY_JSON_OBJECT),
                Arguments.of("${md-name-pattern}", new JnksIotMsgMetaData(Map.of("md-name-pattern", "md-test-asset")), JnksIotMsg.EMPTY_JSON_OBJECT),
                Arguments.of("${msg-name-pattern}", JnksIotMsgMetaData.EMPTY, "{\"msg-name-pattern\":\"msg-test-asset\"}")
        );
    }

    @Test
    public void givenOriginatorSourceIsEntityAndEntityCouldNotFound_whenOnMsg_thenTellFailure() throws JnksIotNodeException {
        config.setOriginatorSource(ENTITY);
        config.setEntityType(EntityType.ASSET.name());
        config.setEntityNamePattern("${md-name-pattern}");

        JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();
        metaData.putValue("md-name-pattern", "test-asset");
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metaData)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .build();

        given(ctxMock.getDbCallbackExecutor()).willReturn(dbExecutor);
        given(ctxMock.getAssetService()).willReturn(assetServiceMock);
        given(ctxMock.getTenantId()).willReturn(TENANT_ID);
        given(assetServiceMock.findAssetByTenantIdAndName(any(TenantId.class), any(String.class))).willReturn(null);

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));
        node.onMsg(ctxMock, msg);

        ArgumentCaptor<Throwable> throwable = ArgumentCaptor.forClass(Throwable.class);
        then(ctxMock).should().tellFailure(eq(msg), throwable.capture());
        assertThat(throwable.getValue()).isInstanceOf(IllegalStateException.class).hasMessage("Failed to find asset with name 'test-asset'!");
    }

}
