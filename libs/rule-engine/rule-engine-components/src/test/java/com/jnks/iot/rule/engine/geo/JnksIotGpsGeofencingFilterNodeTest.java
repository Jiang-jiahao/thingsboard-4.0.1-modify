package com.jnks.iot.rule.engine.geo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.common.util.geo.Coordinates;
import com.jnks.iot.common.util.geo.PerimeterType;
import com.jnks.iot.common.util.geo.RangeUnit;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class JnksIotGpsGeofencingFilterNodeTest {

    private static final double CIRCLE_RANGE = 1.0;
    private static final Coordinates CIRCLE_CENTER = new Coordinates(49.0384, 31.4513);
    private static final Coordinates POINT_INSIDE_CIRCLE = new Coordinates(49.0354, 31.4513); // distance from center: 0.334 km
    private static final Coordinates POINT_OUTSIDE_CIRCLE = new Coordinates(49.0284, 31.4513); // distance from center: 1.112 km

    private JnksIotContext ctx;
    private JnksIotGpsGeofencingFilterNode node;

    @BeforeEach
    void setUp() {
        ctx = mock(JnksIotContext.class);
        node = new JnksIotGpsGeofencingFilterNode();
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    // Exception tests

    @Test
    void givenDefaultConfig_whenOnMsg_thenExceptionInvalidMsg() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsg msg = getEmptyArrayJnksIotMsg(deviceId);

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.onMsg(ctx, msg));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("Incoming Message is not a valid JSON object!");
    }

    @Test
    void givenDefaultConfig_whenOnMsg_thenExceptionMissingPerimeterDefinitionNewVersion() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsg msg = getJnksIotMsg(deviceId, JnksIotMsgMetaData.EMPTY,
                GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLatitude(), GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLongitude());

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.onMsg(ctx, msg));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("Missing perimeter definition!");
    }

    @Test
    void givenTypePolygonAndConfigWithoutPerimeterKeyName_whenOnMsg_thenExceptionMissingPerimeterDefinitionOldVersion() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setPerimeterKeyName(null);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsg msg = getJnksIotMsg(deviceId, JnksIotMsgMetaData.EMPTY,
                GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLatitude(), GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLongitude());

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.onMsg(ctx, msg));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("Missing perimeter definition!");
    }

    // Polygon tests

    @Test
    void givenTypePolygonAndConfigWithoutPerimeterKeyName_whenOnMsg_thenTrue() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setPerimeterKeyName(null);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsgMetaData metadata = getMetadataForOldVersionPolygonPerimeter();
        JnksIotMsg msg = getJnksIotMsg(deviceId, metadata,
                GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLatitude(), GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLongitude());

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
    void givenTypePolygonAndConfigWithoutPerimeterKeyName_whenOnMsg_thenFalse() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setPerimeterKeyName(null);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsgMetaData metadata = getMetadataForOldVersionPolygonPerimeter();
        JnksIotMsg msg = getJnksIotMsg(deviceId, metadata,
                GeoUtilTest.POINT_OUTSIDE_SIMPLE_RECT.getLatitude(), GeoUtilTest.POINT_OUTSIDE_SIMPLE_RECT.getLongitude());

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
    void givenDefaultConfig_whenOnMsg_thenTrue() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsgMetaData metadata = getMetadataForNewVersionPolygonPerimeter();
        JnksIotMsg msg = getJnksIotMsg(deviceId, metadata,
                GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLatitude(), GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLongitude());

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
    void givenDefaultConfig_whenOnMsg_thenFalse() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsgMetaData metadata = getMetadataForNewVersionPolygonPerimeter();
        JnksIotMsg msg = getJnksIotMsg(deviceId, metadata,
                GeoUtilTest.POINT_OUTSIDE_SIMPLE_RECT.getLatitude(), GeoUtilTest.POINT_OUTSIDE_SIMPLE_RECT.getLongitude());

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
    void givenTypePolygonAndConfigWithPolygonDefined_whenOnMsg_thenTrue() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setFetchPerimeterInfoFromMessageMetadata(false);
        config.setPolygonsDefinition(GeoUtilTest.SIMPLE_RECT);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsg msg = getJnksIotMsg(deviceId, JnksIotMsgMetaData.EMPTY,
                GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLatitude(), GeoUtilTest.POINT_INSIDE_SIMPLE_RECT_CENTER.getLongitude());

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
    void givenTypePolygonAndConfigWithPolygonDefined_whenOnMsg_thenFalse() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setFetchPerimeterInfoFromMessageMetadata(false);
        config.setPolygonsDefinition(GeoUtilTest.SIMPLE_RECT);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsg msg = getJnksIotMsg(deviceId, JnksIotMsgMetaData.EMPTY,
                GeoUtilTest.POINT_OUTSIDE_SIMPLE_RECT.getLatitude(), GeoUtilTest.POINT_OUTSIDE_SIMPLE_RECT.getLongitude());

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

    private JnksIotMsgMetaData getMetadataForOldVersionPolygonPerimeter() {
        var metadata = new JnksIotMsgMetaData();
        metadata.putValue("perimeter", GeoUtilTest.SIMPLE_RECT);
        return metadata;
    }

    private JnksIotMsgMetaData getMetadataForNewVersionPolygonPerimeter() {
        var metadata = new JnksIotMsgMetaData();
        metadata.putValue("ss_perimeter", GeoUtilTest.SIMPLE_RECT);
        return metadata;
    }

    // Circle tests

    @Test
    void givenTypeCircleAndConfigWithoutPerimeterKeyName_whenOnMsg_thenTrue() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setPerimeterKeyName(null);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsgMetaData metadata = getMetadataForOldVersionCirclePerimeter();
        JnksIotMsg msg = getJnksIotMsg(deviceId, metadata,
                POINT_INSIDE_CIRCLE.getLatitude(), POINT_INSIDE_CIRCLE.getLongitude());

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
    void givenTypeCircleAndConfigWithoutPerimeterKeyName_whenOnMsg_thenFalse() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setPerimeterKeyName(null);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsgMetaData metadata = getMetadataForOldVersionCirclePerimeter();
        JnksIotMsg msg = getJnksIotMsg(deviceId, metadata,
                POINT_OUTSIDE_CIRCLE.getLatitude(), POINT_OUTSIDE_CIRCLE.getLongitude());

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
    void givenTypeCircle_whenOnMsg_thenTrue() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setPerimeterType(PerimeterType.CIRCLE);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsgMetaData metadata = getMetadataForNewVersionCirclePerimeter();
        JnksIotMsg msg = getJnksIotMsg(deviceId, metadata,
                POINT_INSIDE_CIRCLE.getLatitude(), POINT_INSIDE_CIRCLE.getLongitude());

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
    void givenTypeCircle_whenOnMsg_thenFalse() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setPerimeterType(PerimeterType.CIRCLE);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsgMetaData metadata = getMetadataForNewVersionCirclePerimeter();
        JnksIotMsg msg = getJnksIotMsg(deviceId, metadata,
                POINT_OUTSIDE_CIRCLE.getLatitude(), POINT_OUTSIDE_CIRCLE.getLongitude());

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
    void givenTypeCircleAndConfigWithCircleDefined_whenOnMsg_thenTrue() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setFetchPerimeterInfoFromMessageMetadata(false);
        config.setPerimeterType(PerimeterType.CIRCLE);
        config.setCenterLatitude(CIRCLE_CENTER.getLatitude());
        config.setCenterLongitude(CIRCLE_CENTER.getLongitude());
        config.setRange(CIRCLE_RANGE);
        config.setRangeUnit(RangeUnit.KILOMETER);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsg msg = getJnksIotMsg(deviceId, JnksIotMsgMetaData.EMPTY,
                POINT_INSIDE_CIRCLE.getLatitude(), POINT_INSIDE_CIRCLE.getLongitude());

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
    void givenTypeCircleAndConfigWithCircleDefined_whenOnMsg_thenFalse() throws JnksIotNodeException {
        // GIVEN
        var config = new JnksIotGpsGeofencingFilterNodeConfiguration().defaultConfiguration();
        config.setFetchPerimeterInfoFromMessageMetadata(false);
        config.setPerimeterType(PerimeterType.CIRCLE);
        config.setCenterLatitude(CIRCLE_CENTER.getLatitude());
        config.setCenterLongitude(CIRCLE_CENTER.getLongitude());
        config.setRange(CIRCLE_RANGE);
        config.setRangeUnit(RangeUnit.KILOMETER);
        node.init(ctx, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        DeviceId deviceId = new DeviceId(UUID.randomUUID());
        JnksIotMsg msg = getJnksIotMsg(deviceId, JnksIotMsgMetaData.EMPTY,
                POINT_OUTSIDE_CIRCLE.getLatitude(), POINT_OUTSIDE_CIRCLE.getLongitude());

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

    private JnksIotMsgMetaData getMetadataForOldVersionCirclePerimeter() {
        var metadata = new JnksIotMsgMetaData();
        metadata.putValue("centerLatitude", String.valueOf(CIRCLE_CENTER.getLatitude()));
        metadata.putValue("centerLongitude", String.valueOf(CIRCLE_CENTER.getLongitude()));
        metadata.putValue("range", String.valueOf(CIRCLE_RANGE));
        metadata.putValue("rangeUnit", String.valueOf(RangeUnit.KILOMETER));
        return metadata;
    }

    private JnksIotMsgMetaData getMetadataForNewVersionCirclePerimeter() {
        ObjectNode perimeter = JacksonUtil.newObjectNode();
        perimeter.put("latitude", CIRCLE_CENTER.getLatitude());
        perimeter.put("longitude", CIRCLE_CENTER.getLongitude());
        perimeter.put("radius", CIRCLE_RANGE);
        perimeter.put("radiusUnit", String.valueOf(RangeUnit.KILOMETER));
        var metadata = new JnksIotMsgMetaData();
        metadata.putValue("ss_perimeter", JacksonUtil.toString(perimeter));
        return metadata;
    }

    private JnksIotMsg getJnksIotMsg(EntityId entityId, JnksIotMsgMetaData metadata, double latitude, double longitude) {
        String data = "{\"latitude\": " + latitude + ", \"longitude\": " + longitude + "}";
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(entityId)
                .copyMetaData(metadata)
                .data(data)
                .build();
    }

    private JnksIotMsg getEmptyArrayJnksIotMsg(EntityId entityId) {
        return JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_ATTRIBUTES_REQUEST)
                .originator(entityId)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_ARRAY)
                .build();
    }

}
