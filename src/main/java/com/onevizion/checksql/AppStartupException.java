package com.onevizion.checksql;

public class AppStartupException extends RuntimeException {

    public AppStartupException() {
        super();
    }

    public AppStartupException(String message, Throwable cause) {
        super(message, cause);
    }

    public AppStartupException(String message) {
        super(message);
    }

    public AppStartupException(Throwable cause) {
        super(cause);
    }

}
