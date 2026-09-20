package com.jnks.iot.server.utils;

import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.cf.CalculatedFieldType;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.BasicKvEntry;
import com.jnks.iot.server.common.util.KvProtoUtil;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldEntityCtxIdProto;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldIdProto;
import com.jnks.iot.server.gen.transport.TransportProtos.CalculatedFieldStateProto;
import com.jnks.iot.server.gen.transport.TransportProtos.SingleValueArgumentProto;
import com.jnks.iot.server.gen.transport.TransportProtos.TsDoubleValProto;
import com.jnks.iot.server.gen.transport.TransportProtos.TsRollingArgumentProto;
import com.jnks.iot.server.gen.transport.TransportProtos.TsValueProto;
import com.jnks.iot.server.service.cf.ctx.CalculatedFieldEntityCtxId;
import com.jnks.iot.server.service.cf.ctx.state.CalculatedFieldState;
import com.jnks.iot.server.service.cf.ctx.state.ScriptCalculatedFieldState;
import com.jnks.iot.server.service.cf.ctx.state.SimpleCalculatedFieldState;
import com.jnks.iot.server.service.cf.ctx.state.SingleValueArgumentEntry;
import com.jnks.iot.server.service.cf.ctx.state.TsRollingArgumentEntry;

import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

public class CalculatedFieldUtils {

    public static CalculatedFieldIdProto toProto(CalculatedFieldId cfId) {
        return CalculatedFieldIdProto.newBuilder()
                .setCalculatedFieldIdMSB(cfId.getId().getMostSignificantBits())
                .setCalculatedFieldIdLSB(cfId.getId().getLeastSignificantBits())
                .build();
    }

    public static CalculatedFieldEntityCtxIdProto toProto(CalculatedFieldEntityCtxId ctxId) {
        return CalculatedFieldEntityCtxIdProto.newBuilder()
                .setTenantIdMSB(ctxId.tenantId().getId().getMostSignificantBits())
                .setTenantIdLSB(ctxId.tenantId().getId().getLeastSignificantBits())
                .setCalculatedFieldIdMSB(ctxId.cfId().getId().getMostSignificantBits())
                .setCalculatedFieldIdLSB(ctxId.cfId().getId().getLeastSignificantBits())
                .setEntityType(ctxId.entityId().getEntityType().name())
                .setEntityIdMSB(ctxId.entityId().getId().getMostSignificantBits())
                .setEntityIdLSB(ctxId.entityId().getId().getLeastSignificantBits())
                .build();
    }

    public static CalculatedFieldEntityCtxId fromProto(CalculatedFieldEntityCtxIdProto ctxIdProto) {
        TenantId tenantId = TenantId.fromUUID(new UUID(ctxIdProto.getTenantIdMSB(), ctxIdProto.getTenantIdLSB()));
        EntityId entityId = EntityIdFactory.getByTypeAndUuid(ctxIdProto.getEntityType(), new UUID(ctxIdProto.getEntityIdMSB(), ctxIdProto.getEntityIdLSB()));
        CalculatedFieldId calculatedFieldId = new CalculatedFieldId(new UUID(ctxIdProto.getCalculatedFieldIdMSB(), ctxIdProto.getCalculatedFieldIdLSB()));
        return new CalculatedFieldEntityCtxId(tenantId, calculatedFieldId, entityId);
    }

    public static CalculatedFieldStateProto toProto(CalculatedFieldEntityCtxId stateId, CalculatedFieldState state) {
        CalculatedFieldStateProto.Builder builder = CalculatedFieldStateProto.newBuilder()
                .setId(toProto(stateId))
                .setType(state.getType().name());

        state.getArguments().forEach((argName, argEntry) -> {
            if (argEntry instanceof SingleValueArgumentEntry singleValueArgumentEntry) {
                builder.addSingleValueArguments(toSingleValueArgumentProto(argName, singleValueArgumentEntry));
            } else if (argEntry instanceof TsRollingArgumentEntry rollingArgumentEntry) {
                builder.addRollingValueArguments(toRollingArgumentProto(argName, rollingArgumentEntry));
            }
        });
        return builder.build();
    }

    public static SingleValueArgumentProto toSingleValueArgumentProto(String argName, SingleValueArgumentEntry entry) {
        SingleValueArgumentProto.Builder builder = SingleValueArgumentProto.newBuilder()
                .setArgName(argName);

        if (entry.getKvEntryValue() != null) {
            builder.setValue(KvProtoUtil.toTsValueProto(entry.getTs(), entry.getKvEntryValue()));
        }

        Optional.ofNullable(entry.getVersion()).ifPresent(builder::setVersion);

        return builder.build();
    }

    public static TsRollingArgumentProto toRollingArgumentProto(String argName, TsRollingArgumentEntry entry) {
        TsRollingArgumentProto.Builder builder = TsRollingArgumentProto.newBuilder()
                .setKey(argName)
                .setLimit(entry.getLimit())
                .setTimeWindow(entry.getTimeWindow());

        entry.getTsRecords().forEach((ts, value) -> builder.addTsValue(TsDoubleValProto.newBuilder().setTs(ts).setValue(value).build()));

        return builder.build();
    }

    public static CalculatedFieldState fromProto(CalculatedFieldStateProto proto) {
        if (StringUtils.isEmpty(proto.getType())) {
            return null;
        }

        CalculatedFieldType type = CalculatedFieldType.valueOf(proto.getType());

        CalculatedFieldState state = switch (type) {
            case SIMPLE -> new SimpleCalculatedFieldState();
            case SCRIPT -> new ScriptCalculatedFieldState();
        };

        proto.getSingleValueArgumentsList().forEach(argProto ->
                state.getArguments().put(argProto.getArgName(), fromSingleValueArgumentProto(argProto)));

        if (CalculatedFieldType.SCRIPT.equals(type)) {
            proto.getRollingValueArgumentsList().forEach(argProto ->
                    state.getArguments().put(argProto.getKey(), fromRollingArgumentProto(argProto)));
        }

        return state;
    }

    public static SingleValueArgumentEntry fromSingleValueArgumentProto(SingleValueArgumentProto proto) {
        if (!proto.hasValue()) {
            return new SingleValueArgumentEntry();
        }
        TsValueProto tsValueProto = proto.getValue();
        return new SingleValueArgumentEntry(
                tsValueProto.getTs(),
                (BasicKvEntry) KvProtoUtil.fromTsValueProto(proto.getArgName(), tsValueProto),
                proto.getVersion()
        );
    }

    public static TsRollingArgumentEntry fromRollingArgumentProto(TsRollingArgumentProto proto) {
        TreeMap<Long, Double> tsRecords = new TreeMap<>();
        proto.getTsValueList().forEach(tsValueProto -> tsRecords.put(tsValueProto.getTs(), tsValueProto.getValue()));
        return new TsRollingArgumentEntry(tsRecords, proto.getLimit(), proto.getTimeWindow());
    }

}
