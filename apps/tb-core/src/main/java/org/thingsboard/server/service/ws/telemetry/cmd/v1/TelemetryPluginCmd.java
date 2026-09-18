package org.thingsboard.server.service.ws.telemetry.cmd.v1;

import org.thingsboard.server.service.ws.WsCmd;

/**
 * @author Andrew Shvayka
 */
public interface TelemetryPluginCmd extends WsCmd {

    int getCmdId();

    void setCmdId(int cmdId);

    String getKeys();

}
