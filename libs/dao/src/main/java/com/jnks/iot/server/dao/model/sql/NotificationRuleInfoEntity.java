package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.notification.NotificationDeliveryMethod;
import com.jnks.iot.server.common.data.notification.rule.NotificationRule;
import com.jnks.iot.server.common.data.notification.rule.NotificationRuleInfo;
import com.jnks.iot.server.common.data.notification.template.NotificationTemplateConfig;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode(callSuper = true)
public class NotificationRuleInfoEntity extends NotificationRuleEntity {

    private String templateName;
    private JsonNode templateConfig;

    public NotificationRuleInfoEntity(NotificationRuleEntity ruleEntity, String templateName, Object templateConfig) {
        super(ruleEntity);
        this.templateName = templateName;
        this.templateConfig = (JsonNode) templateConfig;
    }

    @Override
    public NotificationRuleInfo toData() {
        NotificationRule rule = super.toData();
        List<NotificationDeliveryMethod> deliveryMethods = fromJson(templateConfig, NotificationTemplateConfig.class)
                .getDeliveryMethodsTemplates().entrySet().stream()
                .filter(entry -> entry.getValue().isEnabled())
                .map(Map.Entry::getKey).collect(Collectors.toList());
        return new NotificationRuleInfo(rule, templateName, deliveryMethods);
    }

}
