package org.thingsboard.server.common.data.security.model.mfa.provider;

import jakarta.validation.constraints.Min;
import lombok.Data;

@Data
public class BackupCodeTwoFaProviderConfig implements TwoFaProviderConfig {

    @Min(value = 1, message = "must be greater than 0")
    private int codesQuantity;

    @Override
    public TwoFaProviderType getProviderType() {
        return TwoFaProviderType.BACKUP_CODE;
    }

}
