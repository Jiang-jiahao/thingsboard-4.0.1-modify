package com.jnks.iot.rule.engine.notification;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.id.NotificationTemplateId;

import java.util.List;
import java.util.UUID;

@Data
public class JnksIotNotificationNodeConfiguration implements NodeConfiguration<JnksIotNotificationNodeConfiguration> {

    @NotEmpty
    private List<UUID> targets;
    @NotNull
    private NotificationTemplateId templateId;

    @Override
    public JnksIotNotificationNodeConfiguration defaultConfiguration() {
        return new JnksIotNotificationNodeConfiguration();
    }

}
