package com.jnks.iot.server.common.msg.queue;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum ServiceType {

    JNKS_IOT_CORE("TB Core"),
    JNKS_IOT_RULE_ENGINE("TB Rule Engine"),
    JNKS_IOT_TRANSPORT("TB Transport"),
    JS_EXECUTOR("JS Executor"),
    JNKS_IOT_VC_EXECUTOR("TB VC Executor"),
    EDQS("TB Entity Data Query Service");

    private final String label;

    public static ServiceType of(String serviceType) {
        return ServiceType.valueOf(serviceType.replace("-", "_").toUpperCase());
    }

}
