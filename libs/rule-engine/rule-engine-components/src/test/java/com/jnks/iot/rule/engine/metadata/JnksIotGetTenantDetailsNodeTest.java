package com.jnks.iot.rule.engine.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.util.ContactBasedEntityDetails;
import com.jnks.iot.rule.engine.util.JnksIotMsgSource;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.tenant.TenantService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JnksIotGetTenantDetailsNodeTest {

    private static final DeviceId DUMMY_DEVICE_ORIGINATOR = new DeviceId(UUID.randomUUID());
    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private TenantService tenantServiceMock;
    private JnksIotGetTenantDetailsNode node;
    private JnksIotGetTenantDetailsNodeConfiguration config;
    private JnksIotNodeConfiguration nodeConfiguration;
    private JnksIotMsg msg;
    private Tenant tenant;

    @BeforeEach
    public void setUp() {
        node = new JnksIotGetTenantDetailsNode();
        config = new JnksIotGetTenantDetailsNodeConfiguration().defaultConfiguration();
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
        tenant = new Tenant();
        tenant.setId(new TenantId(UUID.randomUUID()));
        tenant.setTitle("Tenant title");
        tenant.setCountry("Tenant country");
        tenant.setCity("Tenant city");
        tenant.setState("Tenant state");
        tenant.setZip("123456");
        tenant.setAddress("Tenant address 1");
        tenant.setAddress2("Tenant address 2");
        tenant.setPhone("+123456789");
        tenant.setEmail("email@tenant.com");
        tenant.setAdditionalInfo(JacksonUtil.toJsonNode("{\"someProperty\":\"someValue\",\"description\":\"Tenant description\"}"));
    }

    @Test
    public void givenConfigWithNullFetchTo_whenInit_thenException() {
        // GIVEN
        config.setDetailsList(List.of(ContactBasedEntityDetails.ID));
        config.setFetchTo(null);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        var exception = assertThrows(JnksIotNodeException.class, () -> node.init(ctxMock, nodeConfiguration));

        // THEN
        assertThat(exception.getMessage()).isEqualTo("FetchTo option can't be null! Allowed values: " + Arrays.toString(JnksIotMsgSource.values()));
        verify(ctxMock, never()).tellSuccess(any());
    }

    @Test
    public void givenDefaultConfig_whenInit_thenOK() {
        // THEN
        assertThat(config.getDetailsList()).isEqualTo(Collections.emptyList());
        assertThat(config.getFetchTo()).isEqualTo(JnksIotMsgSource.DATA);
    }

    @Test
    public void givenCustomConfig_whenInit_thenOK() throws JnksIotNodeException {
        // GIVEN
        config.setDetailsList(List.of(ContactBasedEntityDetails.ID, ContactBasedEntityDetails.PHONE));
        config.setFetchTo(JnksIotMsgSource.METADATA);
        nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));

        // WHEN
        node.init(ctxMock, nodeConfiguration);

        // THEN
        assertThat(node.config).isEqualTo(config);
        assertThat(config.getDetailsList()).isEqualTo(List.of(ContactBasedEntityDetails.ID, ContactBasedEntityDetails.PHONE));
        assertThat(config.getFetchTo()).isEqualTo(JnksIotMsgSource.METADATA);
        assertThat(node.fetchTo).isEqualTo(JnksIotMsgSource.METADATA);
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
    public void givenAllEntityDetailsAndFetchToData_whenOnMsg_thenShouldTellSuccessAndFetchAllToData() {
        // GIVEN
        prepareMsgAndConfig(JnksIotMsgSource.DATA, List.of(ContactBasedEntityDetails.values()));

        mockFindTenant();

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgData = "{\"dataKey1\":123,\"dataKey2\":\"dataValue2\"," +
                "\"tenant_id\":\"" + tenant.getId() + "\"," +
                "\"tenant_title\":\"" + tenant.getTitle() + "\"," +
                "\"tenant_country\":\"" + tenant.getCountry() + "\"," +
                "\"tenant_city\":\"" + tenant.getCity() + "\"," +
                "\"tenant_state\":\"" + tenant.getState() + "\"," +
                "\"tenant_zip\":\"" + tenant.getZip() + "\"," +
                "\"tenant_address\":\"" + tenant.getAddress() + "\"," +
                "\"tenant_address2\":\"" + tenant.getAddress2() + "\"," +
                "\"tenant_phone\":\"" + tenant.getPhone() + "\"," +
                "\"tenant_email\":\"" + tenant.getEmail() + "\"," +
                "\"tenant_additionalInfo\":\"" + tenant.getAdditionalInfo().get("description").asText() + "\"}";

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(expectedMsgData);
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenSomeEntityDetailsAndFetchToMetadata_whenOnMsg_thenShouldTellSuccessAndFetchSomeToMetaData() {
        // GIVEN
        prepareMsgAndConfig(JnksIotMsgSource.METADATA, List.of(ContactBasedEntityDetails.ID, ContactBasedEntityDetails.TITLE, ContactBasedEntityDetails.PHONE));

        mockFindTenant();

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        var expectedMsgMetaData = new JnksIotMsgMetaData(msg.getMetaData().getData());
        expectedMsgMetaData.putValue("tenant_id", tenant.getId().getId().toString());
        expectedMsgMetaData.putValue("tenant_title", tenant.getTitle());
        expectedMsgMetaData.putValue("tenant_phone", tenant.getPhone());

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(expectedMsgMetaData);
    }

    @Test
    public void givenNotPresentEntityDetailsAndFetchToData_whenOnMsg_thenShouldTellSuccessAndFetchNothingToData() {
        // GIVEN
        tenant.setZip(null);
        tenant.setAddress(null);
        tenant.setAddress2(null);

        prepareMsgAndConfig(JnksIotMsgSource.DATA, List.of(ContactBasedEntityDetails.ZIP, ContactBasedEntityDetails.ADDRESS, ContactBasedEntityDetails.ADDRESS2));

        mockFindTenant();

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenDidNotFindTenant_whenOnMsg_thenShouldTellSuccessAndFetchNothingToData() {
        // GIVEN
        prepareMsgAndConfig(JnksIotMsgSource.DATA, List.of(ContactBasedEntityDetails.ZIP, ContactBasedEntityDetails.ADDRESS, ContactBasedEntityDetails.ADDRESS2));

        when(ctxMock.getTenantId()).thenReturn(tenant.getId());
        when(ctxMock.getTenantService()).thenReturn(tenantServiceMock);
        when(tenantServiceMock.findTenantByIdAsync(eq(tenant.getId()), eq(tenant.getId()))).thenReturn(Futures.immediateFuture(null));

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenNullDescriptionAndAddInfoEntityDetails_whenOnMsg_thenShouldTellSuccessAndFetchNothingToData() {
        // GIVEN
        tenant.setAdditionalInfo(JacksonUtil.toJsonNode("{\"someProperty\":\"someValue\",\"description\":null}"));

        prepareMsgAndConfig(JnksIotMsgSource.DATA, List.of(ContactBasedEntityDetails.ADDITIONAL_INFO));

        mockFindTenant();

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        var actualMessageCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);

        verify(ctxMock, times(1)).tellSuccess(actualMessageCaptor.capture());
        verify(ctxMock, never()).tellFailure(any(), any());

        assertThat(actualMessageCaptor.getValue().getData()).isEqualTo(msg.getData());
        assertThat(actualMessageCaptor.getValue().getMetaData()).isEqualTo(msg.getMetaData());
    }

    @Test
    public void givenOldConfig_whenUpgrade_thenShouldReturnTrueResultWithNewConfig() throws Exception {
        var defaultConfig = new JnksIotGetTenantDetailsNodeConfiguration().defaultConfiguration();
        var node = new JnksIotGetTenantDetailsNode();
        String oldConfig = "{\"detailsList\":[],\"addToMetadata\":false}";
        JsonNode configJson = JacksonUtil.toJsonNode(oldConfig);
        JnksIotPair<Boolean, JsonNode> upgrade = node.upgrade(0, configJson);
        Assertions.assertTrue(upgrade.getFirst());
        Assertions.assertEquals(defaultConfig, JacksonUtil.treeToValue(upgrade.getSecond(), defaultConfig.getClass()));
    }

    private void prepareMsgAndConfig(JnksIotMsgSource fetchTo, List<ContactBasedEntityDetails> detailsList) {
        config.setDetailsList(detailsList);
        config.setFetchTo(fetchTo);

        node.config = config;
        node.fetchTo = fetchTo;

        var msgMetaData = new JnksIotMsgMetaData();
        msgMetaData.putValue("metaKey1", "metaValue1");
        msgMetaData.putValue("metaKey2", "metaValue2");

        var msgData = "{\"dataKey1\":123,\"dataKey2\":\"dataValue2\"}";

        msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DUMMY_DEVICE_ORIGINATOR)
                .copyMetaData(msgMetaData)
                .data(msgData)
                .build();
    }

    private void mockFindTenant() {
        when(ctxMock.getTenantId()).thenReturn(tenant.getId());
        when(ctxMock.getTenantService()).thenReturn(tenantServiceMock);
        when(tenantServiceMock.findTenantByIdAsync(eq(tenant.getId()), eq(tenant.getId()))).thenReturn(Futures.immediateFuture(tenant));
    }

}
