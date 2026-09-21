package com.jnks.iot.rule.engine.rpc;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.StringUtils;

@Data
public class JnksIotSendRpcReplyNodeConfiguration implements NodeConfiguration<JnksIotSendRpcReplyNodeConfiguration> {

    public static final String SERVICE_ID = "serviceId";
    public static final String SESSION_ID = "sessionId";
    public static final String REQUEST_ID = "requestId";

    private String serviceIdMetaDataAttribute;
    private String sessionIdMetaDataAttribute;
    private String requestIdMetaDataAttribute;

    @Override
    public JnksIotSendRpcReplyNodeConfiguration defaultConfiguration() {
        JnksIotSendRpcReplyNodeConfiguration configuration = new JnksIotSendRpcReplyNodeConfiguration();
        configuration.setServiceIdMetaDataAttribute(SERVICE_ID);
        configuration.setSessionIdMetaDataAttribute(SESSION_ID);
        configuration.setRequestIdMetaDataAttribute(REQUEST_ID);
        return configuration;
    }

    public String getServiceIdMetaDataAttribute() {
        return !StringUtils.isEmpty(serviceIdMetaDataAttribute) ? serviceIdMetaDataAttribute : SERVICE_ID;
    }

    public String getSessionIdMetaDataAttribute() {
        return !StringUtils.isEmpty(sessionIdMetaDataAttribute) ? sessionIdMetaDataAttribute : SESSION_ID;
    }

    public String getRequestIdMetaDataAttribute() {
        return !StringUtils.isEmpty(requestIdMetaDataAttribute) ? requestIdMetaDataAttribute : REQUEST_ID;
    }
}

