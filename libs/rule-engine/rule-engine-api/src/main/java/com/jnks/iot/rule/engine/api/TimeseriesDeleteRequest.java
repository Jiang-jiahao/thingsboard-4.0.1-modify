package com.jnks.iot.rule.engine.api;

import com.google.common.util.concurrent.FutureCallback;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.kv.DeleteTsKvQuery;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;

import java.util.List;
import java.util.UUID;

@Getter
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TimeseriesDeleteRequest implements CalculatedFieldSystemAwareRequest {

    private final TenantId tenantId;
    private final EntityId entityId;
    private final List<String> keys;
    private final List<DeleteTsKvQuery> deleteHistoryQueries;
    private final List<CalculatedFieldId> previousCalculatedFieldIds;
    private final UUID jnksIotMsgId;
    private final JnksIotMsgType jnksIotMsgType;
    private final FutureCallback<List<String>> callback;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private TenantId tenantId;
        private EntityId entityId;
        private List<String> keys;
        private List<DeleteTsKvQuery> deleteHistoryQueries;
        private List<CalculatedFieldId> previousCalculatedFieldIds;
        private UUID jnksIotMsgId;
        private JnksIotMsgType jnksIotMsgType;
        private FutureCallback<List<String>> callback;

        Builder() {}

        public Builder tenantId(TenantId tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder entityId(EntityId entityId) {
            this.entityId = entityId;
            return this;
        }

        public Builder keys(List<String> keys) {
            this.keys = keys;
            return this;
        }

        public Builder deleteHistoryQueries(List<DeleteTsKvQuery> deleteHistoryQueries) {
            this.deleteHistoryQueries = deleteHistoryQueries;
            return this;
        }

        public Builder previousCalculatedFieldIds(List<CalculatedFieldId> previousCalculatedFieldIds) {
            this.previousCalculatedFieldIds = previousCalculatedFieldIds;
            return this;
        }

        public Builder jnksIotMsgId(UUID jnksIotMsgId) {
            this.jnksIotMsgId = jnksIotMsgId;
            return this;
        }

        public Builder jnksIotMsgType(JnksIotMsgType jnksIotMsgType) {
            this.jnksIotMsgType = jnksIotMsgType;
            return this;
        }

        public Builder callback(FutureCallback<List<String>> callback) {
            this.callback = callback;
            return this;
        }

        public TimeseriesDeleteRequest build() {
            return new TimeseriesDeleteRequest(tenantId, entityId, keys, deleteHistoryQueries, previousCalculatedFieldIds, jnksIotMsgId, jnksIotMsgType, callback);
        }

    }

}
