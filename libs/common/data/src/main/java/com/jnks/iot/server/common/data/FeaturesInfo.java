package com.jnks.iot.server.common.data;

import lombok.Data;

@Data
public class FeaturesInfo {
    boolean isEmailEnabled;
    boolean isSmsEnabled;
    boolean isNotificationEnabled;
    boolean isOauthEnabled;
    boolean isTwoFaEnabled;
}
