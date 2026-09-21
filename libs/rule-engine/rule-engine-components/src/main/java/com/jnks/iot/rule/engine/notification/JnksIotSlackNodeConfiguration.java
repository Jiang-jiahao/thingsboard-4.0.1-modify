package com.jnks.iot.rule.engine.notification;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.notification.targets.slack.SlackConversation;
import com.jnks.iot.server.common.data.notification.targets.slack.SlackConversationType;

@Data
public class JnksIotSlackNodeConfiguration implements NodeConfiguration<JnksIotSlackNodeConfiguration> {

    private String botToken;
    private boolean useSystemSettings;
    @NotEmpty
    private String messageTemplate;

    private SlackConversationType conversationType;
    @NotNull
    @Valid
    private SlackConversation conversation;

    @Override
    public JnksIotSlackNodeConfiguration defaultConfiguration() {
        JnksIotSlackNodeConfiguration config = new JnksIotSlackNodeConfiguration();
        config.setUseSystemSettings(true);
        config.setBotToken("xoxb-");
        config.setMessageTemplate("Device ${deviceId}: temperature is $[temperature]");
        config.setConversationType(SlackConversationType.PUBLIC_CHANNEL);
        return config;
    }

}
