package com.jnks.iot.server.dao.oauth2;

import com.jnks.iot.server.common.data.oauth2.OAuth2Params;
import com.jnks.iot.server.dao.Dao;

public interface OAuth2ParamsDao extends Dao<OAuth2Params> {
    void deleteAll();
}
