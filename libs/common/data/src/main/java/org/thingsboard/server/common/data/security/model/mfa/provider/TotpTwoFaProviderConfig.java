package org.thingsboard.server.common.data.security.model.mfa.provider;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class TotpTwoFaProviderConfig implements TwoFaProviderConfig {

    @NotBlank
    private String issuerName;

    @Override
    public TwoFaProviderType getProviderType() {
        return TwoFaProviderType.TOTP;
    }

}
