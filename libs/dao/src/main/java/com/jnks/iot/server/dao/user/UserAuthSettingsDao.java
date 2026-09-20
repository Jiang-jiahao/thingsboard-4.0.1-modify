package com.jnks.iot.server.dao.user;

import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.security.UserAuthSettings;
import com.jnks.iot.server.dao.Dao;

public interface UserAuthSettingsDao extends Dao<UserAuthSettings> {

    UserAuthSettings findByUserId(UserId userId);

    void removeByUserId(UserId userId);

}
