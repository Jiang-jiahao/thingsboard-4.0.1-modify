package com.jnks.iot.server.common.data;

import com.jnks.iot.server.common.data.debug.DebugSettings;

public interface HasDebugSettings {

    @Deprecated
    boolean isDebugMode();

    @Deprecated
    void setDebugMode(boolean debugMode);

    DebugSettings getDebugSettings();

    void setDebugSettings(DebugSettings debugSettings);

}
