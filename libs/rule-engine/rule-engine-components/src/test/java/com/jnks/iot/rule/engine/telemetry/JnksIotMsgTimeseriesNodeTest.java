package com.jnks.iot.rule.engine.telemetry;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.api.RuleEngineTelemetryService;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.rule.engine.telemetry.strategy.ProcessingStrategy;
import com.jnks.iot.server.common.adaptor.JsonConverter;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.DoubleDataEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import com.jnks.iot.server.common.data.tenant.profile.TenantProfileData;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.ConstraintValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.Advanced;
import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.Deduplicate;
import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.OnEveryMessage;
import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.WebSocketsOnly;

@ExtendWith(MockitoExtension.class)
public class JnksIotMsgTimeseriesNodeTest extends AbstractRuleNodeUpgradeTest {

    private final TenantId TENANT_ID = TenantId.fromUUID(UUID.fromString("c8f34868-603a-4433-876a-7d356e5cf377"));
    private final DeviceId DEVICE_ID = new DeviceId(UUID.fromString("e5095e9a-04f4-44c9-b443-1cf1b97d3384"));

    private TenantProfile tenantProfile;

    private JnksIotMsgTimeseriesNode node;
    private JnksIotMsgTimeseriesNodeConfiguration config;

    @Mock
    private JnksIotContext ctxMock;
    @Mock
    private RuleEngineTelemetryService telemetryServiceMock;

    @BeforeEach
    public void setUp() throws JnksIotNodeException {
        tenantProfile = new TenantProfile(new TenantProfileId(UUID.fromString("ab78dd78-83d0-43fa-869f-d42ec9ed1744")));
        var tenantProfileConfiguration = new DefaultTenantProfileConfiguration();
        tenantProfileConfiguration.setDefaultStorageTtlDays(5);
        var tenantProfileData = new TenantProfileData();
        tenantProfileData.setConfiguration(tenantProfileConfiguration);
        tenantProfile.setProfileData(tenantProfileData);
        lenient().when(ctxMock.getTenantProfile()).thenReturn(tenantProfile);

        lenient().when(ctxMock.getTenantId()).thenReturn(TENANT_ID);
        lenient().when(ctxMock.getTelemetryService()).thenReturn(telemetryServiceMock);

        node = spy(new JnksIotMsgTimeseriesNode());
        config = new JnksIotMsgTimeseriesNodeConfiguration().defaultConfiguration();
    }

    @Test
    public void verifyDefaultConfig() {
        assertThat(config.getDefaultTTL()).isEqualTo(0L);
        assertThat(config.getProcessingSettings()).isInstanceOf(OnEveryMessage.class);
        assertThat(config.isUseServerTs()).isFalse();
    }

    @Test
    public void whenInit_thenShouldAddTenantProfileListener() throws Exception {
        // GIVEN-WHEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        // THEN
        then(ctxMock).should().addTenantProfileListener(any());
    }

    @Test
    public void givenProcessingSettingsAreNull_whenValidatingConstraints_thenThrowsException() {
        // GIVEN
        config.setProcessingSettings(null);

        // WHEN-THEN
        assertThatThrownBy(() -> ConstraintValidator.validateFields(config))
                .isInstanceOf(DataValidationException.class)
                .hasMessage("Validation error: processingSettings must not be null");
    }

    @ParameterizedTest
    @EnumSource(JnksIotMsgType.class)
    public void givenMsgTypeAndEmptyMsgData_whenOnMsg_thenVerifyFailureMsg(JnksIotMsgType msgType) throws JnksIotNodeException {
        // GIVEN
        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(msgType)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(JnksIotMsg.EMPTY_JSON_ARRAY)
                .build();

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        then(ctxMock).should().addTenantProfileListener(any());
        then(ctxMock).should().getTenantProfile();

        ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(ctxMock).tellFailure(eq(msg), throwableCaptor.capture());

        if (JnksIotMsgType.POST_TELEMETRY_REQUEST.equals(msgType)) {
            assertThat(throwableCaptor.getValue()).isInstanceOf(IllegalArgumentException.class).hasMessage("Msg body is empty: " + msg.getData());
            verifyNoMoreInteractions(ctxMock);
            return;
        }
        assertThat(throwableCaptor.getValue()).isInstanceOf(IllegalArgumentException.class).hasMessage("Unsupported msg type: " + msgType);
        verifyNoMoreInteractions(ctxMock);
    }

    @Test
    public void givenTtlFromConfigIsZeroAndUseServerTsIsTrue_whenOnMsg_thenSaveTimeseriesUsingTenantProfileDefaultTtl() throws JnksIotNodeException {
        // GIVEN
        config.setUseServerTs(true);

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        String data = """
                {
                    "temp": 45,
                    "humidity": 77
                }
                """;
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(JnksIotMsgMetaData.EMPTY)
                .data(data)
                .build();

        doAnswer(invocation -> {
            TimeseriesSaveRequest request = invocation.getArgument(0);
            request.getCallback().onSuccess(null);
            return null;
        }).when(telemetryServiceMock).saveTimeseries(any(TimeseriesSaveRequest.class));

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        then(ctxMock).should().getTenantId();
        then(ctxMock).should().getTelemetryService();
        then(ctxMock).should().addTenantProfileListener(any());
        then(ctxMock).should().getTenantProfile();

        List<TsKvEntry> expectedList = getTsKvEntriesListWithTs(data, System.currentTimeMillis());
        verify(telemetryServiceMock).saveTimeseries(assertArg(request -> {
            assertThat(request.getTenantId()).isEqualTo(TENANT_ID);
            assertThat(request.getCustomerId()).isNull();
            assertThat(request.getEntityId()).isEqualTo(DEVICE_ID);
            assertThat(request.getEntries()).usingRecursiveFieldByFieldElementComparatorIgnoringFields("ts").containsExactlyElementsOf(expectedList);
            assertThat(request.getTtl()).isEqualTo(extractTtlAsSeconds(tenantProfile));
            assertThat(request.getStrategy()).isEqualTo(TimeseriesSaveRequest.Strategy.PROCESS_ALL);
            assertThat(request.getCallback()).isInstanceOf(TelemetryNodeCallback.class);
        }));
        verify(ctxMock).tellSuccess(msg);
        verifyNoMoreInteractions(ctxMock, telemetryServiceMock);
    }

    @Test
    public void givenSkipLatestProcessingSettingsAndTtlFromConfig_whenOnMsg_thenSaveTimeseriesUsingTtlFromConfig() throws JnksIotNodeException {
        // GIVEN
        config.setDefaultTTL(10L);

        var timeseries = ProcessingStrategy.onEveryMessage();
        var latest = ProcessingStrategy.skip();
        var webSockets = ProcessingStrategy.onEveryMessage();
        var calculatedFields = ProcessingStrategy.onEveryMessage();
        var processingSettings = new Advanced(timeseries, latest, webSockets, calculatedFields);
        config.setProcessingSettings(processingSettings);

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        String data = """
                {
                    "temp": 45,
                    "humidity": 77
                }
                """;
        long ts = System.currentTimeMillis();
        var metadata = Map.of("ts", String.valueOf(ts));
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(new JnksIotMsgMetaData(metadata))
                .data(data)
                .build();

        doAnswer(invocation -> {
            TimeseriesSaveRequest request = invocation.getArgument(0);
            request.getCallback().onSuccess(null);
            return null;
        }).when(telemetryServiceMock).saveTimeseries(any(TimeseriesSaveRequest.class));

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        then(ctxMock).should().getTenantId();
        then(ctxMock).should().getTelemetryService();
        then(ctxMock).should().addTenantProfileListener(any());
        then(ctxMock).should().getTenantProfile();

        List<TsKvEntry> expectedList = getTsKvEntriesListWithTs(data, ts);
        verify(telemetryServiceMock).saveTimeseries(assertArg(request -> {
            assertThat(request.getTenantId()).isEqualTo(TENANT_ID);
            assertThat(request.getCustomerId()).isNull();
            assertThat(request.getEntityId()).isEqualTo(DEVICE_ID);
            assertThat(request.getEntries()).containsExactlyElementsOf(expectedList);
            assertThat(request.getTtl()).isEqualTo(config.getDefaultTTL());
            assertThat(request.getStrategy()).isEqualTo(new TimeseriesSaveRequest.Strategy(true, false, true, true));
            assertThat(request.getCallback()).isInstanceOf(TelemetryNodeCallback.class);
        }));
        verify(ctxMock).tellSuccess(msg);
        verifyNoMoreInteractions(ctxMock, telemetryServiceMock);
    }

    @ParameterizedTest
    @MethodSource
    public void givenTtlFromConfigAndTtlFromMd_whenOnMsg_thenVerifyTtl(String ttlFromMd, long ttlFromConfig, long expectedTtl) throws JnksIotNodeException {
        // GIVEN
        config.setDefaultTTL(ttlFromConfig);

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        String data = """
                {
                    "temp": 45,
                    "humidity": 77
                }
                """;
        var metadata = new JnksIotMsgMetaData();
        metadata.putValue("TTL", ttlFromMd);
        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .copyMetaData(metadata)
                .data(data)
                .build();

        // WHEN
        node.onMsg(ctxMock, msg);

        // THEN
        verify(telemetryServiceMock).saveTimeseries(assertArg(request -> {
            assertThat(request.getTenantId()).isEqualTo(TENANT_ID);
            assertThat(request.getCustomerId()).isNull();
            assertThat(request.getEntityId()).isEqualTo(DEVICE_ID);
            assertThat(request.getTtl()).isEqualTo(expectedTtl);
            assertThat(request.getStrategy()).isEqualTo(TimeseriesSaveRequest.Strategy.PROCESS_ALL);
            assertThat(request.getCallback()).isInstanceOf(TelemetryNodeCallback.class);
        }));
    }

    private static Stream<Arguments> givenTtlFromConfigAndTtlFromMd_whenOnMsg_thenVerifyTtl() {
        return Stream.of(
                // when ttl is present in metadata and it is not zero then ttl = ttl from metadata
                Arguments.of("1", 2L, 1L),
                // when ttl is absent in metadata and present in config and it is not zero then ttl = ttl from config
                Arguments.of("", 3L, 3L),
                Arguments.of(null, 4L, 4L),
                // when ttl is present in metadata or config but it is zero then ttl = default ttl from tenant profile
                Arguments.of("0", 0L, TimeUnit.DAYS.toSeconds(5L))
        );
    }

    private static List<TsKvEntry> getTsKvEntriesListWithTs(String data, long ts) {
        Map<Long, List<KvEntry>> tsKvMap = JsonConverter.convertToTelemetry(JsonParser.parseString(data), ts);
        List<TsKvEntry> expectedList = new ArrayList<>();
        for (Map.Entry<Long, List<KvEntry>> tsKvEntry : tsKvMap.entrySet()) {
            for (KvEntry kvEntry : tsKvEntry.getValue()) {
                expectedList.add(new BasicTsKvEntry(tsKvEntry.getKey(), kvEntry));
            }
        }
        return expectedList;
    }

    @Test
    public void givenOnEveryMessageProcessingSettingsAndSameMessageTwoTimes_whenOnMsg_thenPersistSameMessageTwoTimes() throws JnksIotNodeException {
        // GIVEN
        config.setProcessingSettings(new OnEveryMessage());

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        var msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", "123")))
                .build();

        // WHEN-THEN
        var expectedSaveRequest = TimeseriesSaveRequest.builder()
                .tenantId(TENANT_ID)
                .customerId(msg.getCustomerId())
                .entityId(msg.getOriginator())
                .entry(new BasicTsKvEntry(123L, new DoubleDataEntry("temperature", 22.3)))
                .ttl(extractTtlAsSeconds(tenantProfile))
                .strategy(TimeseriesSaveRequest.Strategy.PROCESS_ALL)
                .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                .jnksIotMsgId(msg.getId())
                .jnksIotMsgType(msg.getInternalType())
                .build();

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(times(1)).saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest).usingRecursiveComparison().ignoringFields("callback").isEqualTo(expectedSaveRequest)
        ));

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(times(2)).saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest).usingRecursiveComparison().ignoringFields("callback").isEqualTo(expectedSaveRequest)
        ));
    }

    @Test
    public void givenDeduplicateProcessingSettingsAndSameMessageTwoTimes_whenOnMsg_thenPersistThisMessageOnlyFirstTime() throws JnksIotNodeException {
        // GIVEN
        config.setProcessingSettings(new Deduplicate(10));

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        var msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", "123")))
                .build();

        // WHEN-THEN
        var expectedSaveRequest = TimeseriesSaveRequest.builder()
                .tenantId(TENANT_ID)
                .customerId(msg.getCustomerId())
                .entityId(msg.getOriginator())
                .entry(new BasicTsKvEntry(123L, new DoubleDataEntry("temperature", 22.3)))
                .ttl(extractTtlAsSeconds(tenantProfile))
                .strategy(TimeseriesSaveRequest.Strategy.PROCESS_ALL)
                .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                .jnksIotMsgId(msg.getId())
                .jnksIotMsgType(msg.getInternalType())
                .build();

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should().saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest).usingRecursiveComparison().ignoringFields("callback").isEqualTo(expectedSaveRequest)
        ));

        clearInvocations(telemetryServiceMock, ctxMock);

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(never()).saveTimeseries(any());
    }

    @Test
    public void givenWebSocketsOnlyProcessingSettingsAndSameMessageTwoTimes_whenOnMsg_thenSendsOnlyWsUpdateTwoTimes() throws JnksIotNodeException {
        // GIVEN
        config.setProcessingSettings(new WebSocketsOnly());

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        var msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", "123")))
                .build();

        // WHEN-THEN
        var expectedSaveRequest = TimeseriesSaveRequest.builder()
                .tenantId(TENANT_ID)
                .customerId(msg.getCustomerId())
                .entityId(msg.getOriginator())
                .entry(new BasicTsKvEntry(123L, new DoubleDataEntry("temperature", 22.3)))
                .ttl(extractTtlAsSeconds(tenantProfile))
                .strategy(TimeseriesSaveRequest.Strategy.WS_ONLY)
                .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                .jnksIotMsgId(msg.getId())
                .jnksIotMsgType(msg.getInternalType())
                .build();

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(times(1)).saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest).usingRecursiveComparison().ignoringFields("callback").isEqualTo(expectedSaveRequest)
        ));

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(times(2)).saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest).usingRecursiveComparison().ignoringFields("callback").isEqualTo(expectedSaveRequest)
        ));
    }

    @Test
    public void givenAdvancedProcessingSettingsWithOnEveryMessageStrategiesForAllActionsAndSameMessageTwoTimes_whenOnMsg_thenPersistSameMessageTwoTimes() throws JnksIotNodeException {
        // GIVEN
        config.setProcessingSettings(new Advanced(
                ProcessingStrategy.onEveryMessage(),
                ProcessingStrategy.onEveryMessage(),
                ProcessingStrategy.onEveryMessage(),
                ProcessingStrategy.onEveryMessage()
        ));

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        var msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", "123")))
                .build();

        // WHEN-THEN
        var expectedSaveRequest = TimeseriesSaveRequest.builder()
                .tenantId(TENANT_ID)
                .customerId(msg.getCustomerId())
                .entityId(msg.getOriginator())
                .entry(new BasicTsKvEntry(123L, new DoubleDataEntry("temperature", 22.3)))
                .ttl(extractTtlAsSeconds(tenantProfile))
                .strategy(TimeseriesSaveRequest.Strategy.PROCESS_ALL)
                .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                .jnksIotMsgId(msg.getId())
                .jnksIotMsgType(msg.getInternalType())
                .build();

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(times(1)).saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest).usingRecursiveComparison().ignoringFields("callback").isEqualTo(expectedSaveRequest)
        ));

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(times(2)).saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest).usingRecursiveComparison().ignoringFields("callback").isEqualTo(expectedSaveRequest)
        ));
    }

    @Test
    public void givenAdvancedProcessingSettingsWithDifferentDeduplicateStrategyForEachAction_whenOnMsg_thenEvaluatesStrategiesForEachActionsIndependently() throws JnksIotNodeException {
        // GIVEN
        config.setProcessingSettings(new Advanced(
                ProcessingStrategy.deduplicate(1),
                ProcessingStrategy.deduplicate(2),
                ProcessingStrategy.deduplicate(3),
                ProcessingStrategy.deduplicate(4)
        ));

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        long ts1 = 500L;
        long ts2 = 1500L;
        long ts3 = 2500L;
        long ts4 = 3500L;
        long ts5 = 4500L;

        // WHEN-THEN
        node.onMsg(ctxMock, JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", Long.toString(ts1))))
                .build());
        then(telemetryServiceMock).should().saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest.getStrategy()).isEqualTo(TimeseriesSaveRequest.Strategy.PROCESS_ALL)
        ));

        clearInvocations(telemetryServiceMock);

        node.onMsg(ctxMock, JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", Long.toString(ts2))))
                .build());
        then(telemetryServiceMock).should().saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest.getStrategy()).isEqualTo(
                        new TimeseriesSaveRequest.Strategy(true, false, false, false)
                )
        ));

        clearInvocations(telemetryServiceMock);

        node.onMsg(ctxMock, JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", Long.toString(ts3))))
                .build());
        then(telemetryServiceMock).should().saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest.getStrategy()).isEqualTo(
                        new TimeseriesSaveRequest.Strategy(true, true, false, false)
                )
        ));

        clearInvocations(telemetryServiceMock);

        node.onMsg(ctxMock, JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", Long.toString(ts4))))
                .build());
        then(telemetryServiceMock).should().saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest.getStrategy()).isEqualTo(
                        new TimeseriesSaveRequest.Strategy(true, false, true, false)
                )
        ));

        clearInvocations(telemetryServiceMock);

        node.onMsg(ctxMock, JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", Long.toString(ts5))))
                .build());
        then(telemetryServiceMock).should().saveTimeseries(assertArg(
                actualSaveRequest -> assertThat(actualSaveRequest.getStrategy()).isEqualTo(
                        new TimeseriesSaveRequest.Strategy(true, true, false, true)
                )
        ));
    }

    @Test
    public void givenAdvancedProcessingSettingsWithSkipStrategiesForAllActionsAndSameMessageTwoTimes_whenOnMsg_thenSkipsSameMessageTwoTimes() throws JnksIotNodeException {
        // GIVEN
        config.setProcessingSettings(new Advanced(
                ProcessingStrategy.skip(),
                ProcessingStrategy.skip(),
                ProcessingStrategy.skip(),
                ProcessingStrategy.skip()
        ));

        node.init(ctxMock, new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config)));

        var msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(DEVICE_ID)
                .data(JacksonUtil.newObjectNode().put("temperature", 22.3).toString())
                .metaData(new JnksIotMsgMetaData(Map.of("ts", "123")))
                .build();

        // WHEN-THEN
        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(never()).saveTimeseries(any());
        then(ctxMock).should(times(1)).tellSuccess(msg);

        node.onMsg(ctxMock, msg);
        then(telemetryServiceMock).should(never()).saveTimeseries(any());
        then(ctxMock).should(times(2)).tellSuccess(msg);
    }

    private static long extractTtlAsSeconds(TenantProfile tenantProfile) {
        return TimeUnit.DAYS.toSeconds(tenantProfile.getDefaultProfileConfiguration().getDefaultStorageTtlDays());
    }

    @Override
    protected JnksIotNode getTestNode() {
        return node;
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                Arguments.of(0, """
                                {
                                  "defaultTTL": 0,
                                  "useServerTs": false,
                                  "skipLatestPersistence": false
                                }""",
                        true,
                        """
                                {
                                    "defaultTTL": 0,
                                    "useServerTs": false,
                                    "processingSettings": {
                                        "type": "ON_EVERY_MESSAGE"
                                    }
                                }"""),
                Arguments.of(0, """
                                {
                                  "defaultTTL": 0,
                                  "useServerTs": false
                                }""",
                        true,
                        """
                                {
                                    "defaultTTL": 0,
                                    "useServerTs": false,
                                    "processingSettings": {
                                        "type": "ON_EVERY_MESSAGE"
                                    }
                                }"""),
                Arguments.of(0, """
                                {
                                  "defaultTTL": 0,
                                  "useServerTs": false,
                                  "skipLatestPersistence": null
                                }""",
                        true,
                        """
                                {
                                    "defaultTTL": 0,
                                    "useServerTs": false,
                                    "processingSettings": {
                                        "type": "ON_EVERY_MESSAGE"
                                    }
                                }"""),
                Arguments.of(0, """
                                {
                                  "defaultTTL": 0,
                                  "useServerTs": false,
                                  "skipLatestPersistence": true
                                }""",
                        true,
                        """
                                {
                                    "defaultTTL": 0,
                                    "useServerTs": false,
                                    "processingSettings": {
                                        "type": "ADVANCED",
                                        "timeseries": {
                                            "type": "ON_EVERY_MESSAGE"
                                        },
                                        "latest": {
                                            "type": "SKIP"
                                        },
                                        "webSockets": {
                                            "type": "ON_EVERY_MESSAGE"
                                        },
                                        "calculatedFields": {
                                            "type": "ON_EVERY_MESSAGE"
                                        }
                                    }
                                }""")
        );
    }

}
