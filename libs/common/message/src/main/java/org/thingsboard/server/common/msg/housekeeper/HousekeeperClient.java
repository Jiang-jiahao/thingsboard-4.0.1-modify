package org.thingsboard.server.common.msg.housekeeper;

import org.thingsboard.server.common.data.housekeeper.HousekeeperTask;

public interface HousekeeperClient {

    void submitTask(HousekeeperTask task);

}
