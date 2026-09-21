package com.jnks.iot.rule.engine.action;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.DonAsynchron;
import com.jnks.iot.rule.engine.api.AttributesDeleteRequest;
import com.jnks.iot.rule.engine.api.AttributesSaveRequest;
import com.jnks.iot.rule.engine.api.EmptyNodeConfiguration;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.adaptor.JsonConverter;
import com.jnks.iot.server.common.data.AttributeScope;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.objects.AttributesEntityView;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.util.CollectionsUtil;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ACTIVITY_EVENT;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ATTRIBUTES_DELETED;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.ATTRIBUTES_UPDATED;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.INACTIVITY_EVENT;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.POST_ATTRIBUTES_REQUEST;
import static com.jnks.iot.server.common.data.msg.JnksIotNodeConnectionType.SUCCESS;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "copy to view",
        configClazz = EmptyNodeConfiguration.class,
        nodeDescription = "Copy attributes from asset/device to entity view and changes message originator to related entity view",
        nodeDetails = "Copy attributes from asset/device to related entity view according to entity view configuration. \n " +
                "Copy will be done only for attributes that are between start and end dates and according to attribute keys configuration. \n" +
                "Changes message originator to related entity view and produces new messages according to count of updated entity views",
        configDirective = "jnksIotNodeEmptyConfig",
        icon = "content_copy"
)
public class JnksIotCopyAttributesToEntityViewNode implements JnksIotNode {

    EmptyNodeConfiguration config;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        this.config = JnksIotNodeUtils.convert(configuration, EmptyNodeConfiguration.class);
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        if (msg.isTypeOneOf(ATTRIBUTES_UPDATED, ATTRIBUTES_DELETED,
                ACTIVITY_EVENT, INACTIVITY_EVENT, POST_ATTRIBUTES_REQUEST)) {
            if (!msg.getMetaData().getData().isEmpty()) {
                long now = System.currentTimeMillis();
                AttributeScope scope = msg.isTypeOf(POST_ATTRIBUTES_REQUEST) ?
                        AttributeScope.CLIENT_SCOPE : AttributeScope.valueOf(msg.getMetaData().getValue(DataConstants.SCOPE));

                ListenableFuture<List<EntityView>> entityViewsFuture =
                        ctx.getEntityViewService().findEntityViewsByTenantIdAndEntityIdAsync(ctx.getTenantId(), msg.getOriginator());

                DonAsynchron.withCallback(entityViewsFuture,
                        entityViews -> {
                            for (EntityView entityView : entityViews) {
                                long startTime = entityView.getStartTimeMs();
                                long endTime = entityView.getEndTimeMs();
                                if ((endTime != 0 && endTime > now && startTime < now) || (endTime == 0 && startTime < now)) {
                                    if (msg.isTypeOf(ATTRIBUTES_DELETED)) {
                                        List<String> attributes = new ArrayList<>();
                                        for (JsonElement element : JsonParser.parseString(msg.getData()).getAsJsonObject().get("attributes").getAsJsonArray()) {
                                            if (element.isJsonPrimitive()) {
                                                JsonPrimitive value = element.getAsJsonPrimitive();
                                                if (value.isString()) {
                                                    attributes.add(value.getAsString());
                                                }
                                            }
                                        }
                                        List<String> filteredAttributes =
                                                attributes.stream().filter(attr -> attributeContainsInEntityView(scope, attr, entityView)).collect(Collectors.toList());
                                        if (!filteredAttributes.isEmpty()) {
                                            ctx.getTelemetryService().deleteAttributes(AttributesDeleteRequest.builder()
                                                    .tenantId(ctx.getTenantId())
                                                    .entityId(entityView.getId())
                                                    .scope(scope)
                                                    .keys(filteredAttributes)
                                                    .callback(getFutureCallback(ctx, msg, entityView))
                                                    .build());
                                        }
                                    } else {
                                        Set<AttributeKvEntry> attributes = JsonConverter.convertToAttributes(JsonParser.parseString(msg.getData()));
                                        List<AttributeKvEntry> filteredAttributes =
                                                attributes.stream().filter(attr -> attributeContainsInEntityView(scope, attr.getKey(), entityView)).collect(Collectors.toList());
                                        ctx.getTelemetryService().saveAttributes(AttributesSaveRequest.builder()
                                                .tenantId(ctx.getTenantId())
                                                .entityId(entityView.getId())
                                                .scope(scope)
                                                .entries(filteredAttributes)
                                                .callback(getFutureCallback(ctx, msg, entityView))
                                                .build());
                                    }
                                }
                            }
                            ctx.ack(msg);
                        },
                        t -> ctx.tellFailure(msg, t));
            } else {
                ctx.tellFailure(msg, new IllegalArgumentException("Message metadata is empty"));
            }
        } else {
            ctx.tellFailure(msg, new IllegalArgumentException("Unsupported msg type [" + msg.getType() + "]"));
        }
    }

    private FutureCallback<Void> getFutureCallback(JnksIotContext ctx, JnksIotMsg msg, EntityView entityView) {
        return new FutureCallback<Void>() {
            @Override
            public void onSuccess(@Nullable Void result) {
                transformAndTellNext(ctx, msg, entityView);
            }

            @Override
            public void onFailure(Throwable t) {
                ctx.tellFailure(msg, t);
            }
        };
    }

    private void transformAndTellNext(JnksIotContext ctx, JnksIotMsg msg, EntityView entityView) {
        ctx.enqueueForTellNext(ctx.newMsg(msg.getQueueName(), msg.getType(), entityView.getId(), msg.getCustomerId(), msg.getMetaData(), msg.getData()), SUCCESS);
    }

    private boolean attributeContainsInEntityView(AttributeScope scope, String attrKey, EntityView entityView) {
        AttributesEntityView attributesEntityView = entityView.getKeys().getAttributes();
        List<String> keys = null;
        switch (scope) {
            case CLIENT_SCOPE:
                keys = attributesEntityView.getCs();
                break;
            case SERVER_SCOPE:
                keys = attributesEntityView.getSs();
                break;
            case SHARED_SCOPE:
                keys = attributesEntityView.getSh();
                break;
        }
        return CollectionsUtil.contains(keys, attrKey);
    }

}
