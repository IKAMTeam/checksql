package com.onevizion.checksql.exception;

import com.onevizion.checksql.MessageFormatter;

public abstract class OnevizionException extends RuntimeException {

    public OnevizionException() {
        super();
    }

    public OnevizionException(String message, Throwable cause) {
        super(message, cause);
    }

    public OnevizionException(String message) {
        super(message);
    }

    public OnevizionException(Throwable cause) {
        super(cause);
    }

    public OnevizionException(String message, Throwable cause, Object ... params) {
        super(MessageFormatter.format(message, params), cause);
    }

    public OnevizionException(String message, Object ... params) {
        super(MessageFormatter.format(message, params));
    }

}