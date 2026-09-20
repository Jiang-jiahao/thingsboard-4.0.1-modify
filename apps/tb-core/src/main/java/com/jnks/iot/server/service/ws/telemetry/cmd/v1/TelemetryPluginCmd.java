package com.jnks.iot.server.service.ws.telemetry.cmd.v1;

import com.jnks.iot.server.service.ws.WsCmd;

/**
 * @author Andrew Shvayka
 */
public interface TelemetryPluginCmd extends WsCmd {

    int getCmdId();

    void setCmdId(int cmdId);

    String getKeys();

}
