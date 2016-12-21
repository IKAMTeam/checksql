package com.onevizion.checksql.exception;

public class SqlParsingException extends RuntimeException {

    public SqlParsingException() {
        super();
    }

    public SqlParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlParsingException(String message) {
        super(message);
    }

    public SqlParsingException(Throwable cause) {
        super(cause);
    }

}
