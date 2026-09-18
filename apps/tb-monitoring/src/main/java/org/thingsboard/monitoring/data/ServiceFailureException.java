package org.thingsboard.monitoring.data;

public class ServiceFailureException extends RuntimeException {

    public ServiceFailureException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

    public ServiceFailureException(String message) {
        super(message);
    }

}
