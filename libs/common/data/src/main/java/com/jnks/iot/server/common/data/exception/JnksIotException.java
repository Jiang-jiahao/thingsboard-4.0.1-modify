package com.jnks.iot.server.common.data.exception;

public class JnksIotException extends Exception {

    private static final long serialVersionUID = 1L;

    private JnksIotErrorCode errorCode;

    public JnksIotException() {
        super();
    }

    public JnksIotException(JnksIotErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    public JnksIotException(String message, JnksIotErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public JnksIotException(String message, Throwable cause, JnksIotErrorCode errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public JnksIotException(Throwable cause, JnksIotErrorCode errorCode) {
        super(cause);
        this.errorCode = errorCode;
    }

    public JnksIotErrorCode getErrorCode() {
        return errorCode;
    }

}
