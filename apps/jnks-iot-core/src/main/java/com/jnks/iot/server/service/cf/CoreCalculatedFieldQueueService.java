package com.jnks.iot.server.service.cf;

import com.google.common.util.concurrent.FutureCallback;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.rule.engine.api.AttributesDeleteRequest;
import com.jnks.iot.rule.engine.api.AttributesSaveRequest;
import com.jnks.iot.rule.engine.api.TimeseriesDeleteRequest;
import com.jnks.iot.rule.engine.api.TimeseriesSaveRequest;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;
import com.jnks.iot.server.common.data.kv.TimeseriesSaveResult;
import com.jnks.iot.server.common.data.kv.TsKvEntry;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.util.ProtoUtils;
import com.jnks.iot.server.gen.transport.TransportProtos.AttributeScopeProto;
import com.jnks.iot.server.gen.transport.TransportProtos.AttributeValueProto;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldIdProto;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldTelemetryMsgProto;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.TsKvProto;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueMsgMetadata;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.jnks.iot.server.common.util.ProtoUtils.toTsKvProto;

/**
 * jnks-iot-core 侧把遥测变更推到计算字段 Kafka 队列，由 jnks-iot-rule-engine 过滤并计算。
 * Core 没有 CalculatedFieldCtx 缓存，因此对支持的实体类型一律转发。
 */
@Service
@RequiredArgsConstructor
public class CoreCalculatedFieldQueueService implements CalculatedFieldQueueService {

    private static final Set<EntityType> SUPPORTED_ENTITIES = EnumSet.of(
            EntityType.DEVICE, EntityType.ASSET, EntityType.CUSTOMER, EntityType.TENANT
    );

    public static final JnksIotQueueCallback DUMMY_TB_QUEUE_CALLBACK = new JnksIotQueueCallback() {
        @Override
        public void onSuccess(JnksIotQueueMsgMetadata metadata) {
        }

        @Override
        public void onFailure(Throwable t) {
        }
    };

    private final JnksIotClusterService clusterService;

    @Override
    public void pushRequestToQueue(TimeseriesSaveRequest request, TimeseriesSaveResult result, FutureCallback<Void> callback) {
        push(request.getTenantId(), request.getEntityId(), () -> toTelemetryMsg(request, result), callback);
    }

    @Override
    public void pushRequestToQueue(TimeseriesSaveRequest request, FutureCallback<Void> callback) {
        pushRequestToQueue(request, null, callback);
    }

    @Override
    public void pushRequestToQueue(AttributesSaveRequest request, List<Long> result, FutureCallback<Void> callback) {
        push(request.getTenantId(), request.getEntityId(), () -> toTelemetryMsg(request, result), callback);
    }

    @Override
    public void pushRequestToQueue(AttributesSaveRequest request, FutureCallback<Void> callback) {
        pushRequestToQueue(request, null, callback);
    }

    @Override
    public void pushRequestToQueue(AttributesDeleteRequest request, List<String> result, FutureCallback<Void> callback) {
        push(request.getTenantId(), request.getEntityId(), () -> toTelemetryMsg(request, result), callback);
    }

    @Override
    public void pushRequestToQueue(TimeseriesDeleteRequest request, List<String> result, FutureCallback<Void> callback) {
        push(request.getTenantId(), request.getEntityId(), () -> toTelemetryMsg(request, result), callback);
    }

    private void push(TenantId tenantId, EntityId entityId, java.util.function.Supplier<ToCalculatedFieldMsg> msg, FutureCallback<Void> callback) {
        if (EntityType.TENANT.equals(entityId.getEntityType())) {
            tenantId = (TenantId) entityId;
        }
        if (!SUPPORTED_ENTITIES.contains(entityId.getEntityType())) {
            if (callback != null) {
                callback.onSuccess(null);
            }
            return;
        }
        clusterService.pushMsgToCalculatedFields(tenantId, entityId, msg.get(), wrap(callback));
    }

    private ToCalculatedFieldMsg toTelemetryMsg(TimeseriesSaveRequest request, TimeseriesSaveResult result) {
        ToCalculatedFieldMsg.Builder msg = ToCalculatedFieldMsg.newBuilder();
        CalculatedFieldTelemetryMsgProto.Builder telemetryMsg = buildTelemetryMsgProto(
                request.getTenantId(), request.getEntityId(), request.getPreviousCalculatedFieldIds(), request.getJnksIotMsgId(), request.getJnksIotMsgType());
        List<TsKvEntry> entries = request.getEntries();
        List<Long> versions = result != null ? result.getVersions() : Collections.emptyList();
        for (int i = 0; i < entries.size(); i++) {
            TsKvProto.Builder tsProtoBuilder = toTsKvProto(entries.get(i)).toBuilder();
            if (result != null) {
                tsProtoBuilder.setVersion(versions.get(i));
            }
            telemetryMsg.addTsData(tsProtoBuilder.build());
        }
        msg.setTelemetryMsg(telemetryMsg.build());
        return msg.build();
    }

    private ToCalculatedFieldMsg toTelemetryMsg(AttributesSaveRequest request, List<Long> versions) {
        ToCalculatedFieldMsg.Builder msg = ToCalculatedFieldMsg.newBuilder();
        CalculatedFieldTelemetryMsgProto.Builder telemetryMsg = buildTelemetryMsgProto(
                request.getTenantId(), request.getEntityId(), request.getPreviousCalculatedFieldIds(), request.getJnksIotMsgId(), request.getJnksIotMsgType());
        telemetryMsg.setScope(AttributeScopeProto.valueOf(request.getScope().name()));
        List<AttributeKvEntry> entries = request.getEntries();
        for (int i = 0; i < entries.size(); i++) {
            AttributeValueProto.Builder attrProtoBuilder = ProtoUtils.toProto(entries.get(i)).toBuilder();
            if (versions != null) {
                attrProtoBuilder.setVersion(versions.get(i));
            }
            telemetryMsg.addAttrData(attrProtoBuilder.build());
        }
        msg.setTelemetryMsg(telemetryMsg.build());
        return msg.build();
    }

    private ToCalculatedFieldMsg toTelemetryMsg(AttributesDeleteRequest request, List<String> removedKeys) {
        CalculatedFieldTelemetryMsgProto telemetryMsg = buildTelemetryMsgProto(
                request.getTenantId(), request.getEntityId(), request.getPreviousCalculatedFieldIds(), request.getJnksIotMsgId(), request.getJnksIotMsgType())
                .setScope(AttributeScopeProto.valueOf(request.getScope().name()))
                .addAllRemovedAttrKeys(removedKeys).build();
        return ToCalculatedFieldMsg.newBuilder().setTelemetryMsg(telemetryMsg).build();
    }

    private ToCalculatedFieldMsg toTelemetryMsg(TimeseriesDeleteRequest request, List<String> removedKeys) {
        CalculatedFieldTelemetryMsgProto telemetryMsg = buildTelemetryMsgProto(
                request.getTenantId(), request.getEntityId(), request.getPreviousCalculatedFieldIds(), request.getJnksIotMsgId(), request.getJnksIotMsgType())
                .addAllRemovedTsKeys(removedKeys).build();
        return ToCalculatedFieldMsg.newBuilder().setTelemetryMsg(telemetryMsg).build();
    }

    private CalculatedFieldTelemetryMsgProto.Builder buildTelemetryMsgProto(TenantId tenantId, EntityId entityId,
                                                                            List<CalculatedFieldId> calculatedFieldIds, UUID jnksIotMsgId, JnksIotMsgType jnksIotMsgType) {
        CalculatedFieldTelemetryMsgProto.Builder telemetryMsg = CalculatedFieldTelemetryMsgProto.newBuilder();
        if (EntityType.TENANT.equals(entityId.getEntityType())) {
            tenantId = (TenantId) entityId;
        }
        telemetryMsg.setTenantIdMSB(tenantId.getId().getMostSignificantBits());
        telemetryMsg.setTenantIdLSB(tenantId.getId().getLeastSignificantBits());
        telemetryMsg.setEntityType(entityId.getEntityType().name());
        telemetryMsg.setEntityIdMSB(entityId.getId().getMostSignificantBits());
        telemetryMsg.setEntityIdLSB(entityId.getId().getLeastSignificantBits());
        if (calculatedFieldIds != null) {
            for (CalculatedFieldId cfId : calculatedFieldIds) {
                telemetryMsg.addPreviousCalculatedFields(CalculatedFieldIdProto.newBuilder()
                        .setCalculatedFieldIdMSB(cfId.getId().getMostSignificantBits())
                        .setCalculatedFieldIdLSB(cfId.getId().getLeastSignificantBits())
                        .build());
            }
        }
        if (jnksIotMsgId != null) {
            telemetryMsg.setJnksIotMsgIdMSB(jnksIotMsgId.getMostSignificantBits());
            telemetryMsg.setJnksIotMsgIdLSB(jnksIotMsgId.getLeastSignificantBits());
        }
        if (jnksIotMsgType != null) {
            telemetryMsg.setJnksIotMsgType(jnksIotMsgType.name());
        }
        return telemetryMsg;
    }

    private static JnksIotQueueCallback wrap(FutureCallback<Void> callback) {
        if (callback != null) {
            return new FutureCallbackWrapper(callback);
        }
        return DUMMY_TB_QUEUE_CALLBACK;
    }

    private static class FutureCallbackWrapper implements JnksIotQueueCallback {
        private final FutureCallback<Void> callback;

        private FutureCallbackWrapper(FutureCallback<Void> callback) {
            this.callback = callback;
        }

        @Override
        public void onSuccess(JnksIotQueueMsgMetadata metadata) {
            callback.onSuccess(null);
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }

}
