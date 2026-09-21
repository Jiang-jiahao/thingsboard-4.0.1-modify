package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.ListeningExecutor;
import com.jnks.iot.rule.engine.TestDbCallbackExecutor;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.data.RelationsQuery;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.Dashboard;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.asset.Asset;
import com.jnks.iot.server.common.data.id.AssetId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BaseAttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.StringDataEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.relation.EntityRelation;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationEntityTypeFilter;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.dao.relation.RelationService;
import com.jnks.iot.server.dao.timeseries.TimeseriesService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotGetRelatedAttributeNodeTest {

    private static final EntityId DUMMY_DEVICE_ORIGINATOR = new DeviceId(UUID.randomUUID());
    private static final TenantId TENANT_ID = new TenantId(UUID.randomUUID());
    private static final ListeningExecutor DB_EXECUTOR = new TestDbCallbackExecutor();
    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private AttributesService attributesServiceMock;
    @Mock
    private TimeseriesService timeseriesServiceMock;
    @Mock
    private RelationService relationServiceMock;
    @Mock
    private DeviceService deviceServiceMock;
    private JnksIotGetRelatedAttributeNode node;
    private JnksIotGetRelatedDataNodeConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;
    private EntityRelation entityRelation;
    private JnksIotMsg msg;

    @BeforeEach
    public void setUp() {
        node = new JnksIotGetRelatedAttributeNode();
        config = new JnksIotGetRelatedDataNodeConfiguration().defaultConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        entityRelation = new EntityRelation();
    }

    @Test
    public void givenConfigWithNullFetchTo_whenInit_thenException() {
        // GIVEN
        config.setFetchTo(null);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("FetchTo option can't be null! Allowed values: " + Arrays.toString(JnksIotMsgSource.values()));
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenConfigWithNullDataToFetch_whenInit_thenException() {
        // GIVEN
        config.setDataToFetch(null);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("DataToFetch property cannot be null! Supported values are: " + Arrays.toString(DataToFetch.values()));
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenDefaultConfig_whenInit_thenOK() throws JnksIotNodeException {
        // GIVEN

        // WHEN
        node.init(ctxMock, nodeConfiguration);

        // THEN
        var nodeConfig = (JnksIotGetRelatedDataNodeConfiguration) node.config;
        assertThat(nodeConfig).isEqualTo(config);
        assertThat(nodeConfig.getDataMapping()).isEqualTo(Map.of("serialNumber", "sn"));
        assertThat(nodeConfig.getDataToFetch()).isEqualTo(DataToFetch.ATTRIBUTES);
        assertThat(node.fetchTo).isEqualTo(JnksIotMsgSource.METADATA);

        var relationsQuery = new RelationsQuery();
        var relationEntityTypeFilter = new RelationEntityTypeFilter(EntityRelation.CONTAINS_TYPE, Collections.emptyList());
        relationsQuery.setDirection(EntitySearchDirection.FROM);
        relationsQuery.setMaxLevel(1);
        relationsQuery.setFilters(Collections.singletonList(relationEntityTypeFilter));

        assertThat(nodeConfig.getRelationsQuery()).isEqualTo(relationsQuery);
    }

    @Test
    public void givenCustomConfig_whenInit_thenOK() throws JnksIotNodeException {
        // GIVEN
        config.setDataMapping(Map.of(
                "sourceAttr1", "targetKey1",
                "sourceAttr2", "targetKey2",
                "sourceAttr3", "targetKey3"));
        config.setDataToFetch(DataToFetch.LATEST_TELEMETRY);
        config.setFetchTo(JnksIotMsgSource.DATA);

        var relationsQuery = new RelationsQuery();
        var relationEntityTypeFilter = new RelationEntityTypeFilter(EntityRelation.CONTAINS_TYPE, Collections.emptyList());
        relationsQuery.setDirection(EntitySearchDirection.FROM);
        relationsQuery.setMaxLevel(1);
        relationsQuery.setFilters(Collections.singletonList(relationEntityTypeFilter));

        config.setRelationsQuery(relationsQuery);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        node.init(ctxMock, nodeConfiguration);

        // THEN
        var nodeConfig = (JnksIotGetRelatedDataNodeConfiguration) node.config;
        assertThat(nodeConfig).isEqualTo(config);
        assertThat(nodeConfig.getDataMapping()).isEqualTo(Map.of(
                "sourceAttr1", "targetKey1",
                "sourceAttr2", "targetKey2",
                "sourceAttr3", "targetKey3"
        ));
        assertThat(nodeConfig.getDataToFetch()).isEqualTo(DataToFetch.LATEST_TELEMETRY);
        assertThat(node.fetchTo).isEqualTo(JnksIotMsgSource.DATA);
        assertThat(nodeConfig.getRelationsQuery()).isEqualTo(relationsQuery);
    }

    @Test
    public void givenEmptyAttributesMapping_whenInit_thenException() {
        // GIVEN
        var expectedExceptionMessage = "At least one mapping entry should be specified!";

        config.setDataMapping(Collections.emptyMap());
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo(expectedExceptionMessage);
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenMsgDataIsNotAnJsonObjectAndFetchToData_whenOnMsg_thenException() {
        // GIVEN
        node.fetchTo = JnksIotMsgSource.DATA;
        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DUMMY_DEVICE_ORIGINATOR)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_ARRAY)
                .build();

        // WHEN
        var exception = assertThrows(IllegalArgumentException.class, () -> node.onMsg(ctxMock, msg));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("Message body is not an object!");
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenDidNotFindEntity_whenOnMsg_thenShouldTellFailure() {
        // GIVEN
        prepareMsgAndConfig(JnksIotMsgSource.METADATA, DataToFetch.ATTRIBUTES, DUMMY_DEVICE_ORIGINATOR);

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getRelationService()).thenReturn(relationServiceMock);
        doReturn(Futures.immediateFuture(null)).when(relationServiceMock).findByQuery(eq(TENANT_ID), any());

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        var actualExceptionCaptor = ArgumentCaptor.forClass(Throwable.class);

        verify(ctxMock, never()).tellSuccess(any());
        verify(ctxMock, times(1))
                .tellFailure(actualMessageCaptor.capture(), actualExceptionCaptor.capture());

        var actualMessage = actualMessageCaptor.getValue();
        var actualException = actualExceptionCaptor.getValue();

        var expectedExceptionMessage = "Failed to find related entity to message originator using relation query specified in the configuration!";

        assertEquals(msg, actualMessage);
        assertEquals(expectedExceptionMessage, actualException.getMessage());
        assertInstanceOf(NoSuchElementException.class, actualException);
    }

    @Test
    public void givenFetchAttributesToData_whenOnMsg_thenShouldFetchAttributesToData() {
        // GIVEN
        var customer = new Customer(new CustomerId(UUID.randomUUID()));
        var user = new User(new UserId(UUID.randomUUID()));

        prepareMsgAndConfig(JnksIotMsgSource.DATA, DataToFetch.ATTRIBUTES, customer.getId());

        entityRelation.setFrom(customer.getId());
        entityRelation.setTo(user.getId());
        entityRelation.setType(EntityRelation.CONTAINS_TYPE);

        List<AttributeKvEntry> attributes = List.of(
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey1", "sourceValue1"), 1L),
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey2", "sourceValue2"), 2L),
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey3", "sourceValue3"), 3L)
        );
        var expectedPatternProcessedKeysList = List.of("sourceKey1", "sourceKey2", "sourceKey3");

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getRelationService()).thenReturn(relationServiceMock);
        doReturn(Futures.immediateFuture(List.of(entityRelation))).when(relationServiceMock).findByQuery(eq(TENANT_ID), any());

        when(ctxMock.getAttributesService()).thenReturn(attributesServiceMock);
        when(attributesServiceMock.find(eq(TENANT_ID), eq(user.getId()), eq(AttributeScope.SERVER_SCOPE), argThat(new ListMatcher<>(expectedPatternProcessedKeysList))))
                .thenReturn(Futures.immediateFuture(attributes));

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgData = "{\"temp\":42," +
                "\"humidity\":77," +
                "\"messageBodyPattern1\":\"targetKey2\"," +
                "\"messageBodyPattern2\":\"sourceKey3\"," +
                "\"targetKey1\":\"sourceValue1\"," +
                "\"targetKey2\":\"sourceValue2\"," +
                "\"targetKey3\":\"sourceValue3\"}";

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(expectedMsgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenFetchAttributesToMetaData_whenOnMsg_thenShouldFetchAttributesToMetaData() {
        // GIVEN
        var firstCustomer = new Customer(new CustomerId(UUID.randomUUID()));
        var secondCustomer = new Customer(new CustomerId(UUID.randomUUID()));

        prepareMsgAndConfig(JnksIotMsgSource.METADATA, DataToFetch.ATTRIBUTES, firstCustomer.getId());

        entityRelation.setFrom(firstCustomer.getId());
        entityRelation.setTo(secondCustomer.getId());
        entityRelation.setType(EntityRelation.MANAGES_TYPE);

        List<AttributeKvEntry> attributes = List.of(
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey1", "sourceValue1"), 1L),
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey2", "sourceValue2"), 2L),
                new BaseAttributeKvEntry(new StringDataEntry("sourceKey3", "sourceValue3"), 3L)
        );
        var expectedPatternProcessedKeysList = List.of("sourceKey1", "sourceKey2", "sourceKey3");

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getRelationService()).thenReturn(relationServiceMock);
        doReturn(Futures.immediateFuture(List.of(entityRelation))).when(relationServiceMock).findByQuery(eq(TENANT_ID), any());

        when(ctxMock.getAttributesService()).thenReturn(attributesServiceMock);
        when(attributesServiceMock.find(eq(TENANT_ID), eq(secondCustomer.getId()), eq(AttributeScope.SERVER_SCOPE), argThat(new ListMatcher<>(expectedPatternProcessedKeysList))))
                .thenReturn(Futures.immediateFuture(attributes));

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgMetaData = new JnksIotMsgMetaData(Map.of(
                "metaDataPattern1", "sourceKey2",
                "metaDataPattern2", "targetKey3",
                "targetKey1", "sourceValue1",
                "targetKey2", "sourceValue2",
                "targetKey3", "sourceValue3"
        ));

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(expectedMsgMetaData);
    }

    @Test
    public void givenFetchTelemetryToData_whenOnMsg_thenShouldFetchTelemetryToData() {
        // GIVEN
        var dashboard = new Dashboard(new DashboardId(UUID.randomUUID()));
        var entityView = new EntityView(new EntityViewId(UUID.randomUUID()));

        prepareMsgAndConfig(JnksIotMsgSource.DATA, DataToFetch.LATEST_TELEMETRY, dashboard.getId());

        entityRelation.setFrom(dashboard.getId());
        entityRelation.setTo(entityView.getId());
        entityRelation.setType(EntityRelation.CONTAINS_TYPE);

        List<TsKvEntry> timeseries = List.of(
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey1", "sourceValue1")),
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey2", "sourceValue2")),
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey3", "sourceValue3"))
        );
        var expectedPatternProcessedKeysList = List.of("sourceKey1", "sourceKey2", "sourceKey3");

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getRelationService()).thenReturn(relationServiceMock);
        doReturn(Futures.immediateFuture(List.of(entityRelation))).when(relationServiceMock).findByQuery(eq(TENANT_ID), any());

        when(ctxMock.getTimeseriesService()).thenReturn(timeseriesServiceMock);
        when(timeseriesServiceMock.findLatest(eq(TENANT_ID), eq(entityView.getId()), argThat(new ListMatcher<>(expectedPatternProcessedKeysList))))
                .thenReturn(Futures.immediateFuture(timeseries));

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgData = "{\"temp\":42," +
                "\"humidity\":77," +
                "\"messageBodyPattern1\":\"targetKey2\"," +
                "\"messageBodyPattern2\":\"sourceKey3\"," +
                "\"targetKey1\":\"sourceValue1\"," +
                "\"targetKey2\":\"sourceValue2\"," +
                "\"targetKey3\":\"sourceValue3\"}";

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(expectedMsgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenFetchTelemetryToMetaData_whenOnMsg_thenShouldFetchTelemetryToMetaData() {
        // GIVEN
        var tenant = new Tenant(new TenantId(UUID.randomUUID()));
        var device = new Device(new DeviceId(UUID.randomUUID()));

        prepareMsgAndConfig(JnksIotMsgSource.METADATA, DataToFetch.LATEST_TELEMETRY, tenant.getId());

        entityRelation.setFrom(tenant.getId());
        entityRelation.setTo(device.getId());
        entityRelation.setType(EntityRelation.CONTAINS_TYPE);

        List<TsKvEntry> timeseries = List.of(
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey1", "sourceValue1")),
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey2", "sourceValue2")),
                new BasicTsKvEntry(1L, new StringDataEntry("sourceKey3", "sourceValue3"))
        );
        var expectedPatternProcessedKeysList = List.of("sourceKey1", "sourceKey2", "sourceKey3");

        when(ctxMock.getTenantId()).thenReturn(tenant.getId());

        when(ctxMock.getRelationService()).thenReturn(relationServiceMock);
        doReturn(Futures.immediateFuture(List.of(entityRelation))).when(relationServiceMock).findByQuery(eq(tenant.getId()), any());

        when(ctxMock.getTimeseriesService()).thenReturn(timeseriesServiceMock);
        when(timeseriesServiceMock.findLatest(eq(tenant.getId()), eq(device.getId()), argThat(new ListMatcher<>(expectedPatternProcessedKeysList))))
                .thenReturn(Futures.immediateFuture(timeseries));

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgMetaData = new JnksIotMsgMetaData(Map.of(
                "metaDataPattern1", "sourceKey2",
                "metaDataPattern2", "targetKey3",
                "targetKey1", "sourceValue1",
                "targetKey2", "sourceValue2",
                "targetKey3", "sourceValue3"
        ));

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(expectedMsgMetaData);
    }

    @Test
    public void givenFetchFieldsToData_whenOnMsg_thenShouldFetchFieldsToData() {
        // GIVEN
        var device = new Device();
        device.setId(new DeviceId(UUID.randomUUID()));
        device.setName("Device Name");
        var asset = new Asset(new AssetId(UUID.randomUUID()));

        prepareMsgAndConfig(JnksIotMsgSource.DATA, DataToFetch.FIELDS, asset.getId());

        entityRelation.setFrom(asset.getId());
        entityRelation.setTo(device.getId());
        entityRelation.setType(EntityRelation.CONTAINS_TYPE);

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getRelationService()).thenReturn(relationServiceMock);
        doReturn(Futures.immediateFuture(List.of(entityRelation))).when(relationServiceMock).findByQuery(eq(TENANT_ID), any());

        when(ctxMock.getDeviceService()).thenReturn(deviceServiceMock);
        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        when(deviceServiceMock.findDeviceById(eq(TENANT_ID), eq(device.getId()))).thenReturn(device);

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgData = "{\"temp\":42,\"humidity\":77,\"messageBodyPattern\":\"relatedEntityId\"," +
                "\"relatedEntityId\":\"" + device.getId().getId() + "\",\"relatedEntityName\":\"" + device.getName() + "\"}";

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(expectedMsgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenFetchFieldsToMetadata_whenOnMsg_thenShouldFetchFieldsToMetadata() {
        // GIVEN
        var device = new Device();
        device.setId(new DeviceId(UUID.randomUUID()));
        device.setName("Device Name");
        var asset = new Asset(new AssetId(UUID.randomUUID()));

        prepareMsgAndConfig(JnksIotMsgSource.METADATA, DataToFetch.FIELDS, asset.getId());

        entityRelation.setFrom(asset.getId());
        entityRelation.setTo(device.getId());
        entityRelation.setType(EntityRelation.CONTAINS_TYPE);

        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);

        when(ctxMock.getRelationService()).thenReturn(relationServiceMock);
        doReturn(Futures.immediateFuture(List.of(entityRelation))).when(relationServiceMock).findByQuery(eq(TENANT_ID), any());

        when(ctxMock.getDeviceService()).thenReturn(deviceServiceMock);
        when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        when(deviceServiceMock.findDeviceById(eq(TENANT_ID), eq(device.getId()))).thenReturn(device);

        when(ctxMock.getDbCallbackExecutor()).thenReturn(DB_EXECUTOR);

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgMetadata = new JnksIotMsgMetaData(Map.of(
                "metaDataPattern", "relatedEntityName",
                "relatedEntityId", device.getId().getId().toString(),
                "relatedEntityName", device.getName()
        ));

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(expectedMsgMetadata);
    }

    @Test
    public void givenOldConfig_whenUpgrade_thenShouldReturnTrueResultWithNewConfig() throws Exception {
        var defaultConfig = new JnksIotGetRelatedDataNodeConfiguration().defaultConfiguration();
        var node = new JnksIotGetRelatedAttributeNode();
        String oldConfig = "{\"attrMapping\":{\"serialNumber\":\"sn\"}," +
                "\"relationsQuery\":{\"direction\":\"FROM\",\"maxLevel\":1," +
                "\"filters\":[{\"relationType\":\"Contains\",\"entityTypes\":[]}]," +
                "\"fetchLastLevelOnly\":false}," +
                "\"telemetry\":false}";
        JsonNode configJson = JacksonUtil.toJsonNode(oldConfig);
        JnksIotPair<Boolean, JsonNode> upgrade = node.upgrade(0, configJson);
        Assertions.assertTrue(upgrade.getFirst());
        Assertions.assertEquals(defaultConfig, JacksonUtil.treeToValue(upgrade.getSecond(), defaultConfig.getClass()));
    }

    private void prepareMsgAndConfig(JnksIotMsgSource fetchTo, DataToFetch dataToFetch, EntityId originator) {

        config.setDataToFetch(dataToFetch);
        config.setFetchTo(fetchTo);
        node.config = config;
        node.fetchTo = fetchTo;
        var msgMetaData = new JnksIotMsgMetaData();
        String msgData;
        if (dataToFetch.equals(DataToFetch.FIELDS)) {
            config.setDataMapping(Map.of(
                    "id", "$[messageBodyPattern]",
                    "name", "${metaDataPattern}"));
            msgMetaData.putValue("metaDataPattern", "relatedEntityName");
            msgData = "{\"temp\":42,\"humidity\":77,\"messageBodyPattern\":\"relatedEntityId\"}";
        } else {
            config.setDataMapping(Map.of(
                    "sourceKey1", "targetKey1",
                    "${metaDataPattern1}", "$[messageBodyPattern1]",
                    "$[messageBodyPattern2]", "${metaDataPattern2}"));
            msgMetaData.putValue("metaDataPattern1", "sourceKey2");
            msgMetaData.putValue("metaDataPattern2", "targetKey3");
            msgData = "{\"temp\":42,\"humidity\":77,\"messageBodyPattern1\":\"targetKey2\",\"messageBodyPattern2\":\"sourceKey3\"}";
        }

        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .build();
    }

    @RequiredArgsConstructor
    private static class ListMatcher<T> implements ArgumentMatcher<List<T>> {

        private final List<T> expectedList;

        @Override
        public boolean matches(List<T> actualList) {
            if (actualList == expectedList) {
                return true;
            }
            if (actualList.size() != expectedList.size()) {
                return false;
            }
            return actualList.containsAll(expectedList);
        }

    }

}
