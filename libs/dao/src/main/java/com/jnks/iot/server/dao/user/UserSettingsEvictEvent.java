package com.jnks.iot.server.dao.user;

import lombok.Data;
import com.jnks.iot.server.common.data.settings.UserSettingsCompositeKey;

@Data
public class UserSettingsEvictEvent {
    private final UserSettingsCompositeKey key;
}
