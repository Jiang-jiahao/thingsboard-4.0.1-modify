package com.jnks.iot.rule.engine.telemetry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings;
import com.jnks.iot.rule.engine.telemetry.strategy.ProcessingStrategy;
import com.jnks.iot.server.common.adaptor.JsonConverter;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.tenant.profile.DefaultTenantProfileConfiguration;
import com.jnks.iot.server.common.data.util.JnksIotPair;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.Advanced;
import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.Deduplicate;
import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.OnEveryMessage;
import static com.jnks.iot.rule.engine.telemetry.settings.TimeseriesProcessingSettings.WebSocketsOnly;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.POST_TELEMETRY_REQUEST;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "保存时序数据",
        configClazz = JnksIotMsgTimeseriesNodeConfiguration.class,
        nodeDescription = """
                按配置的 TTL 和处理策略保存时序数据。
                """,
        nodeDetails = """
                节点执行四项<strong>动作：</strong>
                <ul>
                  <li><strong>时序数据：</strong>把时序数据写入数据库的 <code>ts_kv</code> 表。</li>
                  <li><strong>最新值：</strong>把时序数据写入数据库的 <code>ts_kv_latest</code> 表。</li>
                  <li><strong>WebSockets：</strong>通知 WebSockets 订阅方时序数据已更新。</li>
                  <li><strong>计算字段：</strong>通知计算字段时序数据已更新。</li>
                </ul>
                
                每项<em>动作</em>都有三种<strong>处理策略</strong>：
                <ul>
                  <li><strong>每条消息都执行：</strong>对每条消息都执行该动作。</li>
                  <li><strong>去重：</strong>在可配置的时间间隔内，只对同一来源方的首条消息执行该动作。</li>
                  <li><strong>跳过：</strong>从不执行该动作。</li>
                </ul>
                
                <strong>处理策略</strong>通过<em>处理设置</em>配置，支持两种模式：
                <ul>
                  <li><strong>基础</strong>
                    <ul>
                      <li><strong>每条消息都执行：</strong>对所有动作应用「每条消息都执行」策略。</li>
                      <li><strong>去重：</strong>对所有动作应用「去重」策略（可指定时间间隔）。</li>
                      <li><strong>仅 WebSockets：</strong>除 WebSocket 通知外，其余动作应用「跳过」策略，WebSocket 通知则应用「每条消息都执行」策略。</li>
                    </ul>
                  </li>
                  <li><strong>高级：</strong>为每项动作单独配置策略。</li>
                </ul>
                
                默认情况下，时间戳取自 <code>metadata.ts</code>。你也可以启用
                <em>使用服务端时间戳</em>，改为始终使用当前服务端时间。这在顺序处理场景下
                尤其有用：消息可能来自多个来源，时间戳会乱序到达。注意数据库层
                可能会忽略属性和最新值的「过期」记录，因此启用<em>使用服务端时间戳</em>
                可以保证顺序正确。
                <br><br>
                TTL 首先取自 <code>metadata.TTL</code>；取不到时使用节点配置里的默认
                TTL；两者都没有时，沿用租户档案的默认值。
                <br><br>
                该节点期望的消息类型是 <code>POST_TELEMETRY_REQUEST</code>。
                <br><br>
                输出连接：<code>Success</code>、<code>Failure</code>。
                """,
        configDirective = "jnksIotActionNodeTimeseriesConfig",
        icon = "file_upload",
        version = 1
)
public class JnksIotMsgTimeseriesNode implements JnksIotNode {

    private JnksIotMsgTimeseriesNodeConfiguration config;
    private JnksIotContext ctx;
    private long tenantProfileDefaultStorageTtl;

    private TimeseriesProcessingSettings processingSettings;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, JnksIotMsgTimeseriesNodeConfiguration.class);
        this.ctx = ctx;
        ctx.addTenantProfileListener(this::onTenantProfileUpdate);
        onTenantProfileUpdate(ctx.getTenantProfile());
        processingSettings = config.getProcessingSettings();
    }

    private void onTenantProfileUpdate(TenantProfile tenantProfile) {
        DefaultTenantProfileConfiguration configuration = (DefaultTenantProfileConfiguration) tenantProfile.getProfileData().getConfiguration();
        tenantProfileDefaultStorageTtl = TimeUnit.DAYS.toSeconds(configuration.getDefaultStorageTtlDays());
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        if (!msg.isTypeOf(POST_TELEMETRY_REQUEST)) {
            ctx.tellFailure(msg, new IllegalArgumentException("Unsupported msg type: " + msg.getType()));
            return;
        }
        long ts = computeTs(msg, config.isUseServerTs());

        TimeseriesSaveRequest.Strategy strategy = determineSaveStrategy(ts, msg.getOriginator().getId());

        // short-circuit
        if (!strategy.saveTimeseries() && !strategy.saveLatest() && !strategy.sendWsUpdate() && !strategy.processCalculatedFields()) {
            ctx.tellSuccess(msg);
            return;
        }

        String src = msg.getData();
        Map<Long, List<KvEntry>> tsKvMap = JsonConverter.convertToTelemetry(JsonParser.parseString(src), ts);
        if (tsKvMap.isEmpty()) {
            ctx.tellFailure(msg, new IllegalArgumentException("Msg body is empty: " + src));
            return;
        }
        List<TsKvEntry> tsKvEntryList = new ArrayList<>();
        for (Map.Entry<Long, List<KvEntry>> tsKvEntry : tsKvMap.entrySet()) {
            for (KvEntry kvEntry : tsKvEntry.getValue()) {
                tsKvEntryList.add(new BasicTsKvEntry(tsKvEntry.getKey(), kvEntry));
            }
        }
        String ttlValue = msg.getMetaData().getValue("TTL");
        long ttl = !StringUtils.isEmpty(ttlValue) ? Long.parseLong(ttlValue) : config.getDefaultTTL();
        if (ttl == 0L) {
            ttl = tenantProfileDefaultStorageTtl;
        }
        ctx.getTelemetryService().saveTimeseries(TimeseriesSaveRequest.builder()
                .tenantId(ctx.getTenantId())
                .customerId(msg.getCustomerId())
                .entityId(msg.getOriginator())
                .entries(tsKvEntryList)
                .ttl(ttl)
                .strategy(strategy)
                .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                .jnksIotMsgId(msg.getId())
                .jnksIotMsgType(msg.getInternalType())
                .callback(new TelemetryNodeCallback(ctx, msg))
                .build());
    }

    public static long computeTs(JnksIotMsg msg, boolean ignoreMetadataTs) {
        return ignoreMetadataTs ? System.currentTimeMillis() : msg.getMetaDataTs();
    }

    private TimeseriesSaveRequest.Strategy determineSaveStrategy(long ts, UUID originatorUuid) {
        if (processingSettings instanceof OnEveryMessage) {
            return TimeseriesSaveRequest.Strategy.PROCESS_ALL;
        }
        if (processingSettings instanceof WebSocketsOnly) {
            return TimeseriesSaveRequest.Strategy.WS_ONLY;
        }
        if (processingSettings instanceof Deduplicate deduplicate) {
            boolean isFirstMsgInInterval = deduplicate.getProcessingStrategy().shouldProcess(ts, originatorUuid);
            return isFirstMsgInInterval ? TimeseriesSaveRequest.Strategy.PROCESS_ALL : TimeseriesSaveRequest.Strategy.SKIP_ALL;
        }
        if (processingSettings instanceof Advanced advanced) {
            return new TimeseriesSaveRequest.Strategy(
                    advanced.timeseries().shouldProcess(ts, originatorUuid),
                    advanced.latest().shouldProcess(ts, originatorUuid),
                    advanced.webSockets().shouldProcess(ts, originatorUuid),
                    advanced.calculatedFields().shouldProcess(ts, originatorUuid)
            );
        }
        // should not happen
        throw new IllegalArgumentException("Unknown processing settings type: " + processingSettings.getClass().getSimpleName());
    }

    @Override
    public void destroy() {
        ctx.removeListeners();
    }

    @Override
    public JnksIotPair<Boolean, JsonNode> upgrade(int fromVersion, JsonNode oldConfiguration) throws JnksIotNodeException {
        boolean hasChanges = false;
        switch (fromVersion) {
            case 0:
                hasChanges = true;
                JsonNode skipLatestPersistence = oldConfiguration.get("skipLatestPersistence");
                if (skipLatestPersistence != null && "true".equals(skipLatestPersistence.asText())) {
                    var skipLatestProcessingSettings = new Advanced(
                            ProcessingStrategy.onEveryMessage(),
                            ProcessingStrategy.skip(),
                            ProcessingStrategy.onEveryMessage(),
                            ProcessingStrategy.onEveryMessage()
                    );
                    ((ObjectNode) oldConfiguration).set("processingSettings", JacksonUtil.valueToTree(skipLatestProcessingSettings));
                } else {
                    ((ObjectNode) oldConfiguration).set("processingSettings", JacksonUtil.valueToTree(new OnEveryMessage()));
                }
                ((ObjectNode) oldConfiguration).remove("skipLatestPersistence");
                break;
            default:
                break;
        }
        return new JnksIotPair<>(hasChanges, oldConfiguration);
    }

}
