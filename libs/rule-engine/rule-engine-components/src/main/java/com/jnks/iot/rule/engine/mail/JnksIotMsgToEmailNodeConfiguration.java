package com.jnks.iot.rule.engine.mail;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

@Data
public class JnksIotMsgToEmailNodeConfiguration implements NodeConfiguration<JnksIotMsgToEmailNodeConfiguration> {

    private String fromTemplate;
    private String toTemplate;
    private String ccTemplate;
    private String bccTemplate;
    private String subjectTemplate;
    private String bodyTemplate;
    private String isHtmlTemplate;
    private String mailBodyType; // Plain Text -> false. HTML - true. Dynamic - value used from isHtmlTemplate.

    @Override
    public JnksIotMsgToEmailNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotMsgToEmailNodeConfiguration();
        configuration.setFromTemplate("info@testmail.org");
        configuration.setToTemplate("${userEmail}");
        configuration.setSubjectTemplate("Device ${deviceType} temperature high");
        configuration.setBodyTemplate("Device ${deviceName} has high temperature $[temperature]");
        configuration.setMailBodyType("false");
        return configuration;
    }
}
