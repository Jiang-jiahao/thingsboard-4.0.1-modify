package com.jnks.iot.rule.engine.filter;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;

import java.util.Arrays;
import java.util.List;

import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.POST_ATTRIBUTES_REQUEST;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.POST_TELEMETRY_REQUEST;
import static com.jnks.iot.server.common.data.msg.JnksIotMsgType.TO_SERVER_RPC_REQUEST;

/**
 * Created by ashvayka on 19.01.18.
 */
@Data
public class JnksIotMsgTypeFilterNodeConfiguration implements NodeConfiguration<JnksIotMsgTypeFilterNodeConfiguration> {

    private List<String> messageTypes;

    @Override
    public JnksIotMsgTypeFilterNodeConfiguration defaultConfiguration() {
        var configuration = new JnksIotMsgTypeFilterNodeConfiguration();
        configuration.setMessageTypes(Arrays.asList(
                POST_ATTRIBUTES_REQUEST.name(),
                POST_TELEMETRY_REQUEST.name(),
                TO_SERVER_RPC_REQUEST.name()));
        return configuration;
    }
}
