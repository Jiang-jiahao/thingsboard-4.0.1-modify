package com.jnks.iot.rule.engine.telemetry;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.AttributesSaveRequest;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.adaptor.JsonConverter;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.BasicTsKvEntry;
import com.jnks.iot.server.common.data.kv.KvEntry;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.jnks.iot.server.common.data.DataConstants.SCOPE;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "计算字段",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "将传入消息推送到计算字段服务",
        nodeDetails = "该节点支持处理计算字段，而无需将传入消息持久化到数据库。 " +
                "默认情况下，计算字段的处理由 <b>保存属性</b> 和 <b>保存时序数据</b> 节点触发。 " +
                "该规则节点接受与这些节点相同的消息，但允许您独立触发计算 " +
                "字段的处理，确保派生数据可以实时计算和使用，而无需将原始消息存储到数据库中。",
        configDirective = "jnksIotNodeEmptyConfig",
        icon = "published_with_changes"
)
public class JnksIotCalculatedFieldsNode implements JnksIotNode {

    private EmptyNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        switch (msg.getInternalType()) {
            case POST_TELEMETRY_REQUEST -> processPostTelemetryRequest(ctx, msg);
            case POST_ATTRIBUTES_REQUEST -> processPostAttributesRequest(ctx, msg);
            default -> ctx.tellFailure(msg, new IllegalArgumentException("Unsupported msg type: " + msg.getType()));
        }
    }

    private void processPostTelemetryRequest(JnksIotContext ctx, JnksIotMsg msg) {
        Map<Long, List<KvEntry>> tsKvMap = JsonConverter.convertToTelemetry(JsonParser.parseString(msg.getData()), System.currentTimeMillis());

        if (tsKvMap.isEmpty()) {
            ctx.tellSuccess(msg);
            return;
        }

        List<TsKvEntry> tsKvEntryList = new ArrayList<>();
        for (Map.Entry<Long, List<KvEntry>> tsKvEntry : tsKvMap.entrySet()) {
            for (KvEntry kvEntry : tsKvEntry.getValue()) {
                tsKvEntryList.add(new BasicTsKvEntry(tsKvEntry.getKey(), kvEntry));
            }
        }

        TimeseriesSaveRequest timeseriesSaveRequest = TimeseriesSaveRequest.builder()
                .tenantId(ctx.getTenantId())
                .customerId(msg.getCustomerId())
                .entityId(msg.getOriginator())
                .entries(tsKvEntryList)
                .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                .jnksIotMsgId(msg.getId())
                .jnksIotMsgType(msg.getInternalType())
                .callback(new TelemetryNodeCallback(ctx, msg))
                .build();

        ctx.getCalculatedFieldQueueService().pushRequestToQueue(timeseriesSaveRequest, timeseriesSaveRequest.getCallback());
    }

    private void processPostAttributesRequest(JnksIotContext ctx, JnksIotMsg msg) {
        List<AttributeKvEntry> newAttributes = new ArrayList<>(JsonConverter.convertToAttributes(JsonParser.parseString(msg.getData())));

        if (newAttributes.isEmpty()) {
            ctx.tellSuccess(msg);
            return;
        }

        AttributesSaveRequest attributesSaveRequest = AttributesSaveRequest.builder()
                .tenantId(ctx.getTenantId())
                .entityId(msg.getOriginator())
                .scope(AttributeScope.valueOf(msg.getMetaData().getValue(SCOPE)))
                .entries(newAttributes)
                .previousCalculatedFieldIds(msg.getPreviousCalculatedFieldIds())
                .jnksIotMsgId(msg.getId())
                .jnksIotMsgType(msg.getInternalType())
                .callback(new TelemetryNodeCallback(ctx, msg))
                .build();
        ctx.getCalculatedFieldQueueService().pushRequestToQueue(attributesSaveRequest, attributesSaveRequest.getCallback());
    }

}
