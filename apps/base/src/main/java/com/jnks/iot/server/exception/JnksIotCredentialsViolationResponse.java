package com.jnks.iot.server.exception;

import io.swagger.v3.oas.annotations.media.Schema;
import org.springframework.http.HttpStatus;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;

@Schema
public class JnksIotCredentialsViolationResponse extends JnksIotErrorResponse {

    protected JnksIotCredentialsViolationResponse(String message) {
        super(message, JnksIotErrorCode.PASSWORD_VIOLATION, HttpStatus.UNAUTHORIZED);
    }

    public static JnksIotCredentialsViolationResponse of(final String message) {
        return new JnksIotCredentialsViolationResponse(message);
    }

}
