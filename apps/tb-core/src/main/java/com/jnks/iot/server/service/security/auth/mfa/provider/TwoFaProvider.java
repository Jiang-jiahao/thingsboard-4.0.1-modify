package com.jnks.iot.server.service.security.auth.mfa.provider;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.security.model.mfa.account.TwoFaAccountConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderType;
import com.jnks.iot.server.service.security.model.SecurityUser;

public interface TwoFaProvider<C extends TwoFaProviderConfig, A extends TwoFaAccountConfig> {

    A generateNewAccountConfig(User user, C providerConfig);

    default void prepareVerificationCode(SecurityUser user, C providerConfig, A accountConfig) throws JnksIotException {}

    boolean checkVerificationCode(SecurityUser user, String code, C providerConfig, A accountConfig);

    default void check(TenantId tenantId) throws JnksIotException {};


    TwoFaProviderType getType();

}
