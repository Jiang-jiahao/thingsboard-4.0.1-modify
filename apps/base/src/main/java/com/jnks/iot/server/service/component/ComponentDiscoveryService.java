package com.jnks.iot.server.service.component;

import com.jnks.iot.server.common.data.plugin.ComponentDescriptor;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.data.rule.RuleChainType;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Andrew Shvayka
 */
public interface ComponentDiscoveryService {

    void discoverComponents();

    Optional<RuleNodeClassInfo> getRuleNodeInfo(String clazz);

    List<RuleNodeClassInfo> getVersionedNodes();

    List<ComponentDescriptor> getComponents(ComponentType type, RuleChainType ruleChainType);

    List<ComponentDescriptor> getComponents(Set<ComponentType> types, RuleChainType ruleChainType);

    Optional<ComponentDescriptor> getComponent(String clazz);
}
