package com.jnks.iot.server.common.msg;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.gen.MsgProtos;
import com.jnks.iot.server.common.msg.queue.JnksIotMsgCallback;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by ashvayka on 13.01.18.
 */
@Data
@Slf4j
public final class JnksIotMsg implements Serializable {

    public static final String EMPTY_JSON_OBJECT = "{}";
    public static final String EMPTY_JSON_ARRAY = "[]";
    public static final String EMPTY_STRING = "";

    private final String queueName;
    private final UUID id;
    private final long ts;
    private final String type;
    private final JnksIotMsgType internalType;
    private final EntityId originator;
    private final CustomerId customerId;
    private final JnksIotMsgMetaData metaData;
    private final JnksIotMsgDataType dataType;
    private final String data;
    private final RuleChainId ruleChainId;
    private final RuleNodeId ruleNodeId;

    private final UUID correlationId;
    private final Integer partition;

    private final List<CalculatedFieldId> previousCalculatedFieldIds;

    @Getter(value = AccessLevel.NONE)
    @JsonIgnore
    //This field is not serialized because we use queues and there is no need to do it
    private final JnksIotMsgProcessingCtx ctx;

    //This field is not serialized because we use queues and there is no need to do it
    @JsonIgnore
    transient private final JnksIotMsgCallback callback;

    public static JnksIotMsgBuilder newMsg() {
        return new JnksIotMsgBuilder();
    }

    public JnksIotMsgBuilder transform() {
        return new JnksIotMsgTransformer(this);
    }

    public JnksIotMsgBuilder copy() {
        return new JnksIotMsgBuilder(this);
    }

    public JnksIotMsg transform(String queueName) {
        return transform()
                .queueName(queueName)
                .resetRuleNodeId()
                .build();
    }

    // used for enqueueForTellNext
    public static JnksIotMsg newMsg(JnksIotMsg jnksIotMsg, String queueName, RuleChainId ruleChainId, RuleNodeId ruleNodeId) {
        return jnksIotMsg.transform()
                .id(UUID.randomUUID())
                .queueName(queueName)
                .metaData(jnksIotMsg.getMetaData())
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .callback(JnksIotMsgCallback.EMPTY)
                .build();
    }

    public JnksIotMsg copyWithNewCtx() {
        return copy()
                .ctx(ctx.copy())
                .callback(JnksIotMsgCallback.EMPTY)
                .build();
    }

    private JnksIotMsg(String queueName, UUID id, long ts, JnksIotMsgType internalType, String type, EntityId originator, CustomerId customerId, JnksIotMsgMetaData metaData, JnksIotMsgDataType dataType, String data,
                  RuleChainId ruleChainId, RuleNodeId ruleNodeId, UUID correlationId, Integer partition, List<CalculatedFieldId> previousCalculatedFieldIds, JnksIotMsgProcessingCtx ctx, JnksIotMsgCallback callback) {
        this.id = id != null ? id : UUID.randomUUID();
        this.queueName = queueName;
        if (ts > 0) {
            this.ts = ts;
        } else {
            this.ts = System.currentTimeMillis();
        }
        this.internalType = internalType != null ? internalType : getInternalType(type);
        this.type = type != null ? type : this.internalType.name();
        this.originator = originator;
        if (customerId == null || customerId.isNullUid()) {
            if (originator != null && originator.getEntityType() == EntityType.CUSTOMER) {
                this.customerId = new CustomerId(originator.getId());
            } else {
                this.customerId = null;
            }
        } else {
            this.customerId = customerId;
        }
        this.metaData = metaData;
        this.dataType = dataType != null ? dataType : JnksIotMsgDataType.JSON;
        this.data = data;
        this.ruleChainId = ruleChainId;
        this.ruleNodeId = ruleNodeId;
        this.correlationId = correlationId;
        this.partition = partition;
        this.previousCalculatedFieldIds = previousCalculatedFieldIds != null
                ? new CopyOnWriteArrayList<>(previousCalculatedFieldIds)
                : new CopyOnWriteArrayList<>();
        this.ctx = ctx != null ? ctx : new JnksIotMsgProcessingCtx();
        this.callback = Objects.requireNonNullElse(callback, JnksIotMsgCallback.EMPTY);
    }

    public static ByteString toByteString(JnksIotMsg msg) {
        return ByteString.copyFrom(toByteArray(msg));
    }

    public static byte[] toByteArray(JnksIotMsg msg) {
        MsgProtos.JnksIotMsgProto.Builder builder = MsgProtos.JnksIotMsgProto.newBuilder();
        builder.setId(msg.getId().toString());
        builder.setTs(msg.getTs());
        builder.setType(msg.getType());
        builder.setEntityType(msg.getOriginator().getEntityType().name());
        builder.setEntityIdMSB(msg.getOriginator().getId().getMostSignificantBits());
        builder.setEntityIdLSB(msg.getOriginator().getId().getLeastSignificantBits());

        if (msg.getCustomerId() != null) {
            builder.setCustomerIdMSB(msg.getCustomerId().getId().getMostSignificantBits());
            builder.setCustomerIdLSB(msg.getCustomerId().getId().getLeastSignificantBits());
        }

        if (msg.getRuleChainId() != null) {
            builder.setRuleChainIdMSB(msg.getRuleChainId().getId().getMostSignificantBits());
            builder.setRuleChainIdLSB(msg.getRuleChainId().getId().getLeastSignificantBits());
        }

        if (msg.getRuleNodeId() != null) {
            builder.setRuleNodeIdMSB(msg.getRuleNodeId().getId().getMostSignificantBits());
            builder.setRuleNodeIdLSB(msg.getRuleNodeId().getId().getLeastSignificantBits());
        }

        if (msg.getMetaData() != null) {
            builder.setMetaData(MsgProtos.JnksIotMsgMetaDataProto.newBuilder().putAllData(msg.getMetaData().getData()).build());
        }

        builder.setDataType(msg.getDataType().ordinal());
        builder.setData(msg.getData());

        if (msg.getCorrelationId() != null) {
            builder.setCorrelationIdMSB(msg.getCorrelationId().getMostSignificantBits());
            builder.setCorrelationIdLSB(msg.getCorrelationId().getLeastSignificantBits());
        }
        if (msg.getPartition() != null) {
            builder.setPartition(msg.getPartition());
        }

        if (msg.getPreviousCalculatedFieldIds() != null) {
            for (CalculatedFieldId calculatedFieldId : msg.getPreviousCalculatedFieldIds()) {
                MsgProtos.CalculatedFieldIdProto calculatedFieldIdProto = MsgProtos.CalculatedFieldIdProto.newBuilder()
                        .setCalculatedFieldIdMSB(calculatedFieldId.getId().getMostSignificantBits())
                        .setCalculatedFieldIdLSB(calculatedFieldId.getId().getLeastSignificantBits())
                        .build();
                builder.addCalculatedFields(calculatedFieldIdProto);
            }
        }

        builder.setCtx(msg.ctx.toProto());
        return builder.build().toByteArray();
    }

    public static JnksIotMsg fromBytes(String queueName, byte[] data, JnksIotMsgCallback callback) {
        try {
            MsgProtos.JnksIotMsgProto proto = MsgProtos.JnksIotMsgProto.parseFrom(data);
            JnksIotMsgMetaData metaData = new JnksIotMsgMetaData(proto.getMetaData().getDataMap());
            EntityId entityId = EntityIdFactory.getByTypeAndUuid(proto.getEntityType(), new UUID(proto.getEntityIdMSB(), proto.getEntityIdLSB()));
            CustomerId customerId = null;
            RuleChainId ruleChainId = null;
            RuleNodeId ruleNodeId = null;
            UUID correlationId = null;
            Integer partition = null;
            List<CalculatedFieldId> calculatedFieldIds = new CopyOnWriteArrayList<>();
            if (proto.getCustomerIdMSB() != 0L && proto.getCustomerIdLSB() != 0L) {
                customerId = new CustomerId(new UUID(proto.getCustomerIdMSB(), proto.getCustomerIdLSB()));
            }
            if (proto.getRuleChainIdMSB() != 0L && proto.getRuleChainIdLSB() != 0L) {
                ruleChainId = new RuleChainId(new UUID(proto.getRuleChainIdMSB(), proto.getRuleChainIdLSB()));
            }
            if (proto.getRuleNodeIdMSB() != 0L && proto.getRuleNodeIdLSB() != 0L) {
                ruleNodeId = new RuleNodeId(new UUID(proto.getRuleNodeIdMSB(), proto.getRuleNodeIdLSB()));
            }
            if (proto.getCorrelationIdMSB() != 0L && proto.getCorrelationIdLSB() != 0L) {
                correlationId = new UUID(proto.getCorrelationIdMSB(), proto.getCorrelationIdLSB());
                partition = proto.getPartition();
            }

            for (MsgProtos.CalculatedFieldIdProto cfIdProto : proto.getCalculatedFieldsList()) {
                CalculatedFieldId calculatedFieldId = new CalculatedFieldId(new UUID(
                        cfIdProto.getCalculatedFieldIdMSB(),
                        cfIdProto.getCalculatedFieldIdLSB()
                ));
                calculatedFieldIds.add(calculatedFieldId);
            }

            JnksIotMsgProcessingCtx ctx;
            if (proto.hasCtx()) {
                ctx = JnksIotMsgProcessingCtx.fromProto(proto.getCtx());
            } else {
                // Backward compatibility with unprocessed messages fetched from queue after update.
                ctx = new JnksIotMsgProcessingCtx(proto.getRuleNodeExecCounter());
            }

            JnksIotMsgDataType dataType = JnksIotMsgDataType.values()[proto.getDataType()];
            return new JnksIotMsg(queueName, UUID.fromString(proto.getId()), proto.getTs(), null, proto.getType(), entityId, customerId,
                    metaData, dataType, proto.getData(), ruleChainId, ruleNodeId, correlationId, partition, calculatedFieldIds, ctx, callback);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Could not parse protobuf for JnksIotMsg", e);
        }
    }

    public int getAndIncrementRuleNodeCounter() {
        return ctx.getAndIncrementRuleNodeCounter();
    }

    public JnksIotMsgCallback getCallback() {
        // May be null in case of deserialization;
        return Objects.requireNonNullElse(callback, JnksIotMsgCallback.EMPTY);
    }

    public void pushToStack(RuleChainId ruleChainId, RuleNodeId ruleNodeId) {
        ctx.push(ruleChainId, ruleNodeId);
    }

    public JnksIotMsgProcessingStackItem popFormStack() {
        return ctx.pop();
    }

    /**
     * Checks if the message is still valid for processing. May be invalid if the message pack is timed-out or canceled.
     *
     * @return 'true' if message is valid for processing, 'false' otherwise.
     */
    public boolean isValid() {
        return getCallback().isMsgValid();
    }

    public long getMetaDataTs() {
        String tsStr = metaData.getValue("ts");
        if (!StringUtils.isEmpty(tsStr)) {
            try {
                return Long.parseLong(tsStr);
            } catch (NumberFormatException ignored) {
            }
        }
        return ts;
    }

    private JnksIotMsgType getInternalType(String type) {
        if (type != null) {
            try {
                return JnksIotMsgType.valueOf(type);
            } catch (IllegalArgumentException ignored) {
            }
        }
        return JnksIotMsgType.NA;
    }

    public boolean isTypeOf(JnksIotMsgType jnksIotMsgType) {
        return internalType.equals(jnksIotMsgType);
    }

    public boolean isTypeOneOf(JnksIotMsgType... types) {
        for (JnksIotMsgType type : types) {
            if (isTypeOf(type)) {
                return true;
            }
        }
        return false;
    }

    public static class JnksIotMsgTransformer extends JnksIotMsgBuilder {

        JnksIotMsgTransformer(JnksIotMsg jnksIotMsg) {
            super(jnksIotMsg);
        }

        /*
         * metadata is only copied if specified explicitly during transform
         * */
        @Override
        public JnksIotMsgTransformer metaData(JnksIotMsgMetaData metaData) {
            this.metaData = metaData.copy();
            return this;
        }

        /*
         * setting ruleNodeId to null when updating ruleChainId
         * */
        @Override
        public JnksIotMsgBuilder ruleChainId(RuleChainId ruleChainId) {
            this.ruleChainId = ruleChainId;
            this.ruleNodeId = null;
            return this;
        }

        @Override
        public JnksIotMsg build() {
            /*
             * always copying ctx when transforming
             * */
            if (this.ctx != null) {
                this.ctx = this.ctx.copy();
            }
            return super.build();
        }

    }

    public static class JnksIotMsgBuilder {

        protected String queueName;
        protected UUID id;
        protected long ts;
        protected String type;
        protected JnksIotMsgType internalType;
        protected EntityId originator;
        protected CustomerId customerId;
        protected JnksIotMsgMetaData metaData;
        protected JnksIotMsgDataType dataType;
        protected String data;
        protected RuleChainId ruleChainId;
        protected RuleNodeId ruleNodeId;
        protected UUID correlationId;
        protected Integer partition;
        protected List<CalculatedFieldId> previousCalculatedFieldIds;
        protected JnksIotMsgProcessingCtx ctx;
        protected JnksIotMsgCallback callback;

        JnksIotMsgBuilder() {}

        JnksIotMsgBuilder(JnksIotMsg jnksIotMsg) {
            this.queueName = jnksIotMsg.queueName;
            this.id = jnksIotMsg.id;
            this.ts = jnksIotMsg.ts;
            this.type = jnksIotMsg.type;
            this.internalType = jnksIotMsg.internalType;
            this.originator = jnksIotMsg.originator;
            this.customerId = jnksIotMsg.customerId;
            this.metaData = jnksIotMsg.metaData;
            this.dataType = jnksIotMsg.dataType;
            this.data = jnksIotMsg.data;
            this.ruleChainId = jnksIotMsg.ruleChainId;
            this.ruleNodeId = jnksIotMsg.ruleNodeId;
            this.correlationId = jnksIotMsg.correlationId;
            this.partition = jnksIotMsg.partition;
            this.previousCalculatedFieldIds = jnksIotMsg.previousCalculatedFieldIds;
            this.ctx = jnksIotMsg.ctx;
            this.callback = jnksIotMsg.callback;
        }

        public JnksIotMsgBuilder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public JnksIotMsgBuilder id(UUID id) {
            this.id = id;
            return this;
        }

        public JnksIotMsgBuilder ts(long ts) {
            this.ts = ts;
            return this;
        }

        /**
         * <p><strong>Deprecated:</strong> This should only be used when you need to specify a custom message type that doesn't exist in the {@link JnksIotMsgType} enum.
         * Prefer using {@link #type(JnksIotMsgType)} instead.
         */
        @Deprecated
        public JnksIotMsgBuilder type(String type) {
            this.type = type;
            this.internalType = null;
            return this;
        }

        public JnksIotMsgBuilder type(JnksIotMsgType internalType) {
            this.internalType = internalType;
            this.type = internalType.name();
            return this;
        }

        public JnksIotMsgBuilder originator(EntityId originator) {
            this.originator = originator;
            return this;
        }

        public JnksIotMsgBuilder customerId(CustomerId customerId) {
            this.customerId = customerId;
            return this;
        }

        public JnksIotMsgBuilder metaData(JnksIotMsgMetaData metaData) {
            this.metaData = metaData;
            return this;
        }

        public JnksIotMsgBuilder copyMetaData(JnksIotMsgMetaData metaData) {
            this.metaData = metaData.copy();
            return this;
        }

        public JnksIotMsgBuilder dataType(JnksIotMsgDataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public JnksIotMsgBuilder data(String data) {
            this.data = data;
            return this;
        }

        public JnksIotMsgBuilder ruleChainId(RuleChainId ruleChainId) {
            this.ruleChainId = ruleChainId;
            return this;
        }

        public JnksIotMsgBuilder ruleNodeId(RuleNodeId ruleNodeId) {
            this.ruleNodeId = ruleNodeId;
            return this;
        }

        public JnksIotMsgBuilder resetRuleNodeId() {
            return ruleNodeId(null);
        }

        public JnksIotMsgBuilder correlationId(UUID correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public JnksIotMsgBuilder partition(Integer partition) {
            this.partition = partition;
            return this;
        }

        public JnksIotMsgBuilder previousCalculatedFieldIds(List<CalculatedFieldId> previousCalculatedFieldIds) {
            this.previousCalculatedFieldIds = new CopyOnWriteArrayList<>(previousCalculatedFieldIds);
            return this;
        }

        public JnksIotMsgBuilder ctx(JnksIotMsgProcessingCtx ctx) {
            this.ctx = ctx;
            return this;
        }

        public JnksIotMsgBuilder callback(JnksIotMsgCallback callback) {
            this.callback = callback;
            return this;
        }

        public JnksIotMsg build() {
            return new JnksIotMsg(queueName, id, ts, internalType, type, originator, customerId, metaData, dataType, data, ruleChainId, ruleNodeId, correlationId, partition, previousCalculatedFieldIds, ctx, callback);
        }

        public String toString() {
            return "JnksIotMsg.JnksIotMsgBuilder(queueName=" + this.queueName + ", id=" + this.id + ", ts=" + this.ts +
                    ", type=" + this.type + ", internalType=" + this.internalType + ", originator=" + this.originator +
                    ", customerId=" + this.customerId + ", metaData=" + this.metaData + ", dataType=" + this.dataType +
                    ", data=" + this.data + ", ruleChainId=" + this.ruleChainId + ", ruleNodeId=" + this.ruleNodeId +
                    ", correlationId=" + this.correlationId + ", partition=" + this.partition + ", previousCalculatedFields=" + this.previousCalculatedFieldIds +
                    ", ctx=" + this.ctx + ", callback=" + this.callback + ")";
        }

    }

}
