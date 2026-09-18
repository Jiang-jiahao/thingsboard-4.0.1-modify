package org.thingsboard.server.common.msg.queue;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum ServiceType {

    TB_CORE("TB Core"),
    TB_RULE_ENGINE("TB Rule Engine"),
    TB_TRANSPORT("TB Transport"),
    JS_EXECUTOR("JS Executor"),
    TB_VC_EXECUTOR("TB VC Executor"),
    EDQS("TB Entity Data Query Service");

    private final String label;

    public static ServiceType of(String serviceType) {
        return ServiceType.valueOf(serviceType.replace("-", "_").toUpperCase());
    }

}
