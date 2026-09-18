package org.thingsboard.server.service.update;

import org.thingsboard.server.common.data.UpdateMessage;

/**
 * 平台版本更新检查接口。
 */
public interface UpdateService {

    /** 返回最近一次检查到的版本更新信息。 */
    UpdateMessage checkUpdates();

}
