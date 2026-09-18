package org.thingsboard.server.common.data.security.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Schema
@Data
public class SecuritySettings implements Serializable {

    @Serial
    private static final long serialVersionUID = -1307613974597312465L;

    @Schema(description = "用户密码策略对象")
    private UserPasswordPolicy passwordPolicy;

    @Schema(description = "锁定用户帐户前允许登录失败的最大次数")
    private Integer maxFailedLoginAttempts;

    @Schema(description = "用于通知锁定用户的电子邮件")
    private String userLockoutNotificationEmail;

    @Schema(description = "移动秘钥长度")
    private Integer mobileSecretKeyLength;

    @NotNull @Min(1) @Max(24)
    @Schema(description = "用户激活链接的TTL（小时）", minimum = "1", maximum = "24", requiredMode = Schema.RequiredMode.REQUIRED)
    private Integer userActivationTokenTtl;

    @NotNull @Min(1) @Max(24)
    @Schema(description = "以小时为单位的TTL密码重置链接", minimum = "1", maximum = "24", requiredMode = Schema.RequiredMode.REQUIRED)
    private Integer passwordResetTokenTtl;

}
