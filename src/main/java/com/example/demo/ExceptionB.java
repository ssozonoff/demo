package com.example.demo;

public class ExceptionB extends RuntimeException {
    public ExceptionB() {
    }

    public ExceptionB(String message) {
        super(message);
    }

    public ExceptionB(String message, Throwable cause) {
        super(message, cause);
    }

    public ExceptionB(Throwable cause) {
        super(cause);
    }

    public ExceptionB(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
