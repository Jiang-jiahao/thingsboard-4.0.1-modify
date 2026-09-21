package com.jnks.iot.server.service.subscription;

import com.jnks.iot.server.common.data.AttributeScope;

public enum JnksIotAttributeSubscriptionScope {

    ANY_SCOPE(),
    CLIENT_SCOPE(AttributeScope.CLIENT_SCOPE),
    SHARED_SCOPE(AttributeScope.SHARED_SCOPE),
    SERVER_SCOPE(AttributeScope.SERVER_SCOPE);

    private final AttributeScope attributeScope;

    JnksIotAttributeSubscriptionScope() {
        this.attributeScope = null;
    }

    JnksIotAttributeSubscriptionScope(AttributeScope attributeScope) {
        this.attributeScope = attributeScope;
    }

    public AttributeScope getAttributeScope() {
        return attributeScope;
    }

    public static JnksIotAttributeSubscriptionScope of(AttributeScope attributeScope) {
        for (JnksIotAttributeSubscriptionScope scope : JnksIotAttributeSubscriptionScope.values()) {
            if (attributeScope == scope.getAttributeScope()) {
                return scope;
            }
        }
        throw new IllegalArgumentException("Unknown AttributeScope: " + attributeScope.name());
    }


}
