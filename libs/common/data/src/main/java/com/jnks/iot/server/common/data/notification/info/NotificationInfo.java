package com.jnks.iot.server.common.data.notification.info;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.jnks.iot.server.common.data.id.DashboardId;
import com.jnks.iot.server.common.data.id.EntityId;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "type")
public interface NotificationInfo {

    @JsonIgnore
    Map<String, String> getTemplateData();

    default EntityId getStateEntityId() {
        return null;
    }

    default DashboardId getDashboardId() {
        return null;
    }

}
