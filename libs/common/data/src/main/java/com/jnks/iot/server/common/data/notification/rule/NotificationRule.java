package com.jnks.iot.server.common.data.notification.rule;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.BaseData;
import com.jnks.iot.server.common.data.ExportableEntity;
import com.jnks.iot.server.common.data.HasName;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.id.NotificationRuleId;
import com.jnks.iot.server.common.data.id.NotificationTemplateId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerConfig;
import com.jnks.iot.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;
import com.jnks.iot.server.common.data.validation.Length;
import com.jnks.iot.server.common.data.validation.NoXss;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class NotificationRule extends BaseData<NotificationRuleId> implements HasTenantId, HasName, ExportableEntity<NotificationRuleId>, Serializable {

    private TenantId tenantId;
    @NotBlank
    @NoXss
    @Length(max = 255, message = "cannot be longer than 255 chars")
    private String name;
    private boolean enabled;
    @NotNull
    private NotificationTemplateId templateId;

    @NotNull
    private NotificationRuleTriggerType triggerType;
    @NotNull
    @Valid
    private NotificationRuleTriggerConfig triggerConfig;
    @NotNull
    @Valid
    private NotificationRuleRecipientsConfig recipientsConfig;

    private NotificationRuleConfig additionalConfig;

    private NotificationRuleId externalId;

    public NotificationRule(NotificationRule other) {
        super(other);
        this.tenantId = other.tenantId;
        this.name = other.name;
        this.enabled = other.enabled;
        this.templateId = other.templateId;
        this.triggerType = other.triggerType;
        this.triggerConfig = other.triggerConfig;
        this.recipientsConfig = other.recipientsConfig;
        this.additionalConfig = other.additionalConfig;
        this.externalId = other.externalId;
    }

    @JsonIgnore
    @AssertTrue(message = "trigger type not matching")
    public boolean isValid() {
        return triggerType == triggerConfig.getTriggerType() &&
                triggerType == recipientsConfig.getTriggerType();
    }

    @JsonIgnore
    public String getDeduplicationKey() {
        String targets = recipientsConfig.getTargetsTable().values().stream()
                .flatMap(List::stream).sorted().map(Object::toString)
                .collect(Collectors.joining(","));
        return String.join(":", targets, triggerConfig.getDeduplicationKey());
    }

}
