package com.jnks.iot.server.common.msg.housekeeper;

import com.jnks.iot.server.common.data.housekeeper.HousekeeperTask;

public interface HousekeeperClient {

    void submitTask(HousekeeperTask task);

}
