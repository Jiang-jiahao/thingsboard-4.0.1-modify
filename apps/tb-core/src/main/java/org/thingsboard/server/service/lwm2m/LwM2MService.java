package org.thingsboard.server.service.lwm2m;

import org.thingsboard.server.common.data.device.profile.lwm2m.bootstrap.LwM2MServerSecurityConfigDefault;

/**
 * LwM2M 服务接口：向 Core 侧提供 LwM2M 服务器/引导服务器安全配置查询。
 */
public interface LwM2MService {

    /** 获取 LwM2M 服务器或引导服务器的默认安全配置。 */
    LwM2MServerSecurityConfigDefault getServerSecurityInfo(boolean bootstrapServer);

}
