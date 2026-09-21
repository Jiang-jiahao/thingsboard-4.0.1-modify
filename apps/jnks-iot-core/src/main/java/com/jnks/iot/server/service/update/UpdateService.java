package com.jnks.iot.server.service.update;

import com.jnks.iot.server.common.data.UpdateMessage;

/**
 * 平台版本更新检查接口。
 */
public interface UpdateService {

    /** 返回最近一次检查到的版本更新信息。 */
    UpdateMessage checkUpdates();

}
