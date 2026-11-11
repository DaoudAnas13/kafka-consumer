package com.example.productnotification.exception;

/** Non-retryable: no point retrying — send straight to the DLT. */
public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException(String message) {
        super(message);
    }
}