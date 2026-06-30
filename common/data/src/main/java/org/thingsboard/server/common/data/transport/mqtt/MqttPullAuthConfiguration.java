/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.mqtt;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.StringUtils;

import java.io.Serializable;

@Data
public class MqttPullAuthConfiguration implements Serializable {

    private MqttPullAuthType authType = MqttPullAuthType.NONE;
    private String username;
    private String password;

    @JsonIgnore
    public void validate() {
        if (authType == MqttPullAuthType.USERNAME_PASSWORD) {
            if (StringUtils.isBlank(username)) {
                throw new IllegalArgumentException("MQTT pull username/password auth requires username");
            }
        }
    }
}
