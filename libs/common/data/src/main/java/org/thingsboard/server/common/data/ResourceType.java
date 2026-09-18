package org.thingsboard.server.common.data;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum ResourceType {
    LWM2M_MODEL("application/xml", false, false),
    JKS("application/x-java-keystore", false, false),
    PKCS_12("application/x-pkcs12", false, false),
    JS_MODULE("application/javascript", true, true),
    IMAGE(null, true, true),
    DASHBOARD("application/json", true, true);

    @Getter
    private final String mediaType;
    @Getter
    private final boolean customerAccess;
    @Getter
    private final boolean updatable;

}
