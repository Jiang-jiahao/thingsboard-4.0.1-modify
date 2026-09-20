package com.jnks.iot.server.common.data.security.model.mfa.account;

import lombok.Data;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderType;

import java.util.LinkedHashMap;

@Data
public class AccountTwoFaSettings {
    private LinkedHashMap<TwoFaProviderType, TwoFaAccountConfig> configs;
}
