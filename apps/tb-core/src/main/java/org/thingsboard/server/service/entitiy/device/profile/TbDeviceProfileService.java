package org.thingsboard.server.service.entitiy.device.profile;

import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.service.entitiy.SimpleTbEntityService;

/**
 * 设备配置（Device Profile）业务层契约。
 * <p>
 * 由 DeviceProfileController 调用；实现类委托 DAO 并写审计日志。
 */
public interface TbDeviceProfileService extends SimpleTbEntityService<DeviceProfile> {

    /** 将指定配置设为租户默认设备配置。 */
    DeviceProfile setDefaultDeviceProfile(DeviceProfile deviceProfile, DeviceProfile previousDefaultDeviceProfile, User user) throws ThingsboardException;
}
