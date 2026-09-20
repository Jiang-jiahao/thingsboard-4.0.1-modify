package com.jnks.iot.server.common.data.widget;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Value;
import com.jnks.iot.server.common.data.EntityInfo;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.EntityIdFactory;

import java.io.Serial;
import java.util.UUID;

@Value
@EqualsAndHashCode(callSuper = true)
public class WidgetBundleInfo extends EntityInfo {

    @Serial
    private static final long serialVersionUID = 2132305394634509820L;

    public WidgetBundleInfo(@JsonProperty("id") UUID uuid, @JsonProperty("name") String name) {
        super(EntityIdFactory.getByTypeAndUuid(EntityType.WIDGETS_BUNDLE, uuid), name);
    }

}
