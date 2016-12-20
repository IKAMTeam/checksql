package com.onevizion.checksql;

public class UnexpectedException extends OnevizionException {

    public UnexpectedException() {
        super();
    }

    public UnexpectedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnexpectedException(String message) {
        super(message);
    }

    public UnexpectedException(Throwable cause) {
        super(cause);
    }

    public UnexpectedException(String message, Throwable cause, Object ... params) {
        super(message, cause, params);
    }

    public UnexpectedException(String message, Object ... params) {
        super(message, params);
    }

}