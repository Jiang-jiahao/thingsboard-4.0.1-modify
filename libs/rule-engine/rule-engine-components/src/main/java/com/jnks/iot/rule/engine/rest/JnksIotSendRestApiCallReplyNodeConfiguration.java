package com.jnks.iot.rule.engine.rest;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.StringUtils;

@Data
public class JnksIotSendRestApiCallReplyNodeConfiguration implements NodeConfiguration<JnksIotSendRestApiCallReplyNodeConfiguration> {
    public static final String SERVICE_ID = "serviceId";
    public static final String REQUEST_UUID = "requestUUID";

    private String serviceIdMetaDataAttribute;
    private String requestIdMetaDataAttribute;

    @Override
    public JnksIotSendRestApiCallReplyNodeConfiguration defaultConfiguration() {
        JnksIotSendRestApiCallReplyNodeConfiguration configuration = new JnksIotSendRestApiCallReplyNodeConfiguration();
        configuration.setRequestIdMetaDataAttribute(REQUEST_UUID);
        configuration.setServiceIdMetaDataAttribute(SERVICE_ID);
        return configuration;
    }

    public String getServiceIdMetaDataAttribute() {
        return !StringUtils.isEmpty(serviceIdMetaDataAttribute) ? serviceIdMetaDataAttribute : SERVICE_ID;
    }

    public String getRequestIdMetaDataAttribute() {
        return !StringUtils.isEmpty(requestIdMetaDataAttribute) ? requestIdMetaDataAttribute : REQUEST_UUID;
    }
}
