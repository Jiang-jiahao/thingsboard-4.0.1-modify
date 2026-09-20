package com.jnks.iot.server.common.data.notification.targets.platform;

import lombok.Data;

@Data
public class OriginatorEntityOwnerUsersFilter implements UsersFilter {

    @Override
    public UsersFilterType getType() {
        return UsersFilterType.ORIGINATOR_ENTITY_OWNER_USERS;
    }

}
