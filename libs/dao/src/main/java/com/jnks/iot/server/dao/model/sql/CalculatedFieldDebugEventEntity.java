package com.jnks.iot.server.dao.model.sql;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.event.CalculatedFieldDebugEvent;
import com.jnks.iot.server.common.data.id.CalculatedFieldId;
import com.jnks.iot.server.common.data.id.EntityIdFactory;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.dao.model.BaseEntity;

import java.util.UUID;

import static com.jnks.iot.server.dao.model.ModelConstants.CALCULATED_FIELD_DEBUG_EVENT_TABLE_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.EVENT_CALCULATED_FIELD_ARGUMENTS_COLUMN_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.EVENT_CALCULATED_FIELD_ID_COLUMN_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.EVENT_CALCULATED_FIELD_RESULT_COLUMN_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.EVENT_ENTITY_ID_COLUMN_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.EVENT_ENTITY_TYPE_COLUMN_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.EVENT_ERROR_COLUMN_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.EVENT_MSG_ID_COLUMN_NAME;
import static com.jnks.iot.server.dao.model.ModelConstants.EVENT_MSG_TYPE_COLUMN_NAME;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = CALCULATED_FIELD_DEBUG_EVENT_TABLE_NAME)
@NoArgsConstructor
public class CalculatedFieldDebugEventEntity extends EventEntity<CalculatedFieldDebugEvent> implements BaseEntity<CalculatedFieldDebugEvent> {

    @Column(name = EVENT_CALCULATED_FIELD_ID_COLUMN_NAME)
    private UUID calculatedFieldId;
    @Column(name = EVENT_ENTITY_ID_COLUMN_NAME)
    private UUID eventEntityId;
    @Column(name = EVENT_ENTITY_TYPE_COLUMN_NAME)
    private String eventEntityType;
    @Column(name = EVENT_MSG_ID_COLUMN_NAME)
    private UUID msgId;
    @Column(name = EVENT_MSG_TYPE_COLUMN_NAME)
    private String msgType;
    @Column(name = EVENT_CALCULATED_FIELD_ARGUMENTS_COLUMN_NAME)
    private String arguments;
    @Column(name = EVENT_CALCULATED_FIELD_RESULT_COLUMN_NAME)
    private String result;
    @Column(name = EVENT_ERROR_COLUMN_NAME)
    private String error;

    public CalculatedFieldDebugEventEntity(CalculatedFieldDebugEvent event) {
        super(event);
        if (event.getCalculatedFieldId() != null) {
            this.calculatedFieldId = event.getCalculatedFieldId().getId();
        }
        if (event.getEventEntity() != null) {
            this.eventEntityId = event.getEventEntity().getId();
            this.eventEntityType = event.getEventEntity().getEntityType().name();
        }
        this.msgId = event.getMsgId();
        this.msgType = event.getMsgType();
        this.arguments = event.getArguments();
        this.result = event.getResult();
        this.error = event.getError();
    }

    @Override
    public CalculatedFieldDebugEvent toData() {
        var builder = CalculatedFieldDebugEvent.builder()
                .id(id)
                .tenantId(TenantId.fromUUID(tenantId))
                .ts(ts)
                .serviceId(serviceId)
                .entityId(entityId)
                .msgId(msgId)
                .msgType(msgType)
                .arguments(arguments)
                .result(result)
                .error(error);
        if (calculatedFieldId != null) {
            builder.calculatedFieldId(new CalculatedFieldId(calculatedFieldId));
        }
        if (eventEntityId != null) {
            builder.eventEntity(EntityIdFactory.getByTypeAndUuid(eventEntityType, eventEntityId));
        }
        return builder.build();
    }

}
