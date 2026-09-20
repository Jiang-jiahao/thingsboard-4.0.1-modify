package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.widget.BaseWidgetType;
import com.jnks.iot.server.common.data.widget.WidgetType;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ModelConstants.WIDGET_TYPE_TABLE_NAME)
public final class WidgetTypeEntity extends AbstractWidgetTypeEntity<WidgetType> {

    @Convert(converter = JsonConverter.class)
    @Column(name = ModelConstants.WIDGET_TYPE_DESCRIPTOR_PROPERTY)
    private JsonNode descriptor;

    public WidgetTypeEntity() {
        super();
    }

    @Override
    public WidgetType toData() {
        BaseWidgetType baseWidgetType = super.toBaseWidgetType();
        WidgetType widgetType = new WidgetType(baseWidgetType);
        widgetType.setDescriptor(descriptor);
        return widgetType;
    }

}
