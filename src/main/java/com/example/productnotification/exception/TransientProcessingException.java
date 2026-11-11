package com.example.productnotification.exception;

/** Retryable: may succeed on a later attempt (network, temp outage…). */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}