package org.thingsboard.server.common.data.notification.rule;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.thingsboard.server.common.data.notification.rule.trigger.config.NotificationRuleTriggerType;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@JsonIgnoreProperties
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "triggerType", visible = true, include = JsonTypeInfo.As.EXISTING_PROPERTY, defaultImpl = DefaultNotificationRuleRecipientsConfig.class)
@JsonSubTypes({
        @Type(name = "ALARM", value = EscalatedNotificationRuleRecipientsConfig.class),
})
@Data
public abstract class NotificationRuleRecipientsConfig implements Serializable {

    @NotNull
    private NotificationRuleTriggerType triggerType;

    @JsonIgnore
    public abstract Map<Integer, List<UUID>> getTargetsTable();

}
