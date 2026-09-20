package com.jnks.iot.server.common.data.notification.info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.plugin.ComponentLifecycleEvent;

import java.util.Map;

import static com.jnks.iot.server.common.data.util.CollectionsUtil.mapOf;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RuleEngineComponentLifecycleEventNotificationInfo implements RuleOriginatedNotificationInfo {

    private RuleChainId ruleChainId;
    private String ruleChainName;
    private EntityId componentId;
    private String componentName;
    private String action;
    private ComponentLifecycleEvent eventType;
    private String error;

    @Override
    public Map<String, String> getTemplateData() {
        return mapOf(
                "ruleChainId", ruleChainId.toString(),
                "ruleChainName", ruleChainName,
                "componentId", componentId.toString(),
                "componentType", componentId.getEntityType().getNormalName(),
                "componentName", componentName,
                "action", action,
                "eventType", eventType.name().toLowerCase(),
                "error", error
        );
    }

    @Override
    public EntityId getStateEntityId() {
        return ruleChainId;
    }

}
