package com.jnks.iot.server.exception;

import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.http.HttpStatus;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;

@Schema
public class JnksIotCredentialsExpiredResponse extends JnksIotErrorResponse {

    private final String resetToken;

    protected JnksIotCredentialsExpiredResponse(String message, String resetToken) {
        super(message, JnksIotErrorCode.CREDENTIALS_EXPIRED, HttpStatus.UNAUTHORIZED);
        this.resetToken = resetToken;
    }

    public static JnksIotCredentialsExpiredResponse of(final String message, final String resetToken) {
        return new JnksIotCredentialsExpiredResponse(message, resetToken);
    }

    @Schema(description = "Password reset token", accessMode = Schema.AccessMode.READ_ONLY)
    public String getResetToken() {
        return resetToken;
    }
}
